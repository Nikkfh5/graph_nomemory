#include "cli_common.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <iostream>
#include <limits>
#include <system_error>
#include <utility>

#include <cerrno>
#include <signal.h>
#include <unistd.h>

namespace tbank::cli {
namespace {

[[nodiscard]] bool is_allowed(
    const std::span<const std::string_view> allowed,
    const std::string_view name
) {
    return std::find(allowed.begin(), allowed.end(), name) != allowed.end();
}

[[noreturn]] void option_error(
    const std::string_view option,
    const std::string_view detail
) {
    throw UsageError(
        "option --" + std::string(option) + ": " + std::string(detail)
    );
}

[[nodiscard]] char hexadecimal_digit(const unsigned int value) noexcept {
    constexpr std::string_view digits = "0123456789abcdef";
    return digits[value & 0x0fU];
}

[[nodiscard]] bool is_utf8_continuation(const unsigned char byte) noexcept {
    return byte >= 0x80U && byte <= 0xbfU;
}

[[nodiscard]] std::size_t valid_utf8_sequence_length(
    const std::string_view value,
    const std::size_t offset
) noexcept {
    const auto first = static_cast<unsigned char>(value[offset]);
    const std::size_t remaining = value.size() - offset;
    if (first >= 0xc2U && first <= 0xdfU && remaining >= 2U
        && is_utf8_continuation(
            static_cast<unsigned char>(value[offset + 1U])
        )) {
        return 2U;
    }
    if (remaining >= 3U) {
        const auto second = static_cast<unsigned char>(value[offset + 1U]);
        const auto third = static_cast<unsigned char>(value[offset + 2U]);
        const bool valid_tail = is_utf8_continuation(third);
        if (((first == 0xe0U && second >= 0xa0U && second <= 0xbfU)
             || ((first >= 0xe1U && first <= 0xecU)
                 && is_utf8_continuation(second))
             || (first == 0xedU && second >= 0x80U && second <= 0x9fU)
             || ((first >= 0xeeU && first <= 0xefU)
                 && is_utf8_continuation(second)))
            && valid_tail) {
            return 3U;
        }
    }
    if (remaining >= 4U) {
        const auto second = static_cast<unsigned char>(value[offset + 1U]);
        const auto third = static_cast<unsigned char>(value[offset + 2U]);
        const auto fourth = static_cast<unsigned char>(value[offset + 3U]);
        if (((first == 0xf0U && second >= 0x90U && second <= 0xbfU)
             || ((first >= 0xf1U && first <= 0xf3U)
                 && is_utf8_continuation(second))
             || (first == 0xf4U && second >= 0x80U && second <= 0x8fU))
            && is_utf8_continuation(third)
            && is_utf8_continuation(fourth)) {
            return 4U;
        }
    }
    return 0U;
}

void write_all(const int descriptor, const std::string_view payload) {
    std::size_t offset = 0U;
    while (offset < payload.size()) {
        ssize_t written = -1;
        do {
            errno = 0;
            written = ::write(
                descriptor,
                payload.data() + offset,
                payload.size() - offset
            );
        } while (written == -1 && errno == EINTR);
        if (written == -1) {
            throw std::system_error(
                errno == 0 ? EIO : errno,
                std::generic_category(),
                "write CLI result channel"
            );
        }
        if (written == 0) {
            throw std::system_error(
                EIO,
                std::generic_category(),
                "zero-length write to CLI result channel"
            );
        }
        offset += static_cast<std::size_t>(written);
    }
}

[[nodiscard]] bool consume_canonical_unsigned_component(
    const std::string_view value,
    std::size_t& offset
) noexcept {
    if (offset >= value.size()) {
        return false;
    }
    if (value[offset] == '0') {
        ++offset;
        return offset >= value.size()
            || value[offset] < '0' || value[offset] > '9';
    }
    if (value[offset] < '1' || value[offset] > '9') {
        return false;
    }
    do {
        ++offset;
    } while (offset < value.size()
             && value[offset] >= '0' && value[offset] <= '9');
    return true;
}

[[nodiscard]] bool is_canonical_decimal_float(
    const std::string_view value
) noexcept {
    if (value.empty()) {
        return false;
    }
    std::size_t offset = value.front() == '-' ? 1U : 0U;
    if (!consume_canonical_unsigned_component(value, offset)) {
        return false;
    }
    if (offset < value.size() && value[offset] == '.') {
        ++offset;
        const std::size_t fractional_begin = offset;
        while (offset < value.size()
               && value[offset] >= '0' && value[offset] <= '9') {
            ++offset;
        }
        if (offset == fractional_begin) {
            return false;
        }
    }
    if (offset < value.size()
        && (value[offset] == 'e' || value[offset] == 'E')) {
        ++offset;
        if (offset < value.size() && value[offset] == '-') {
            ++offset;
        }
        if (!consume_canonical_unsigned_component(value, offset)) {
            return false;
        }
    }
    return offset == value.size();
}

}  // namespace

ParsedOptions::ParsedOptions(std::map<std::string, std::string> values)
    : values_(std::move(values)) {}

ParsedOptions ParsedOptions::parse(
    const int argument_count,
    char* arguments[],
    const std::span<const std::string_view> allowed_options
) {
    if (argument_count < 1 || arguments == nullptr) {
        throw UsageError("invalid process argument vector");
    }

    std::map<std::string, std::string> values;
    for (int index = 1; index < argument_count; ++index) {
        const std::string_view token(arguments[index]);
        if (!token.starts_with("--") || token.size() == 2U) {
            throw UsageError(
                "unexpected positional or malformed argument: "
                + std::string(token)
            );
        }

        const std::string_view body = token.substr(2U);
        const std::size_t equals = body.find('=');
        const std::string_view name = body.substr(0U, equals);
        if (name.empty() || !is_allowed(allowed_options, name)) {
            throw UsageError("unknown option: --" + std::string(name));
        }

        std::string_view value;
        if (equals != std::string_view::npos) {
            value = body.substr(equals + 1U);
        } else {
            if (index + 1 >= argument_count) {
                option_error(name, "missing value");
            }
            const std::string_view candidate(arguments[index + 1]);
            if (candidate.starts_with("--")) {
                option_error(name, "missing value");
            }
            ++index;
            value = candidate;
        }
        if (value.empty()) {
            option_error(name, "empty value");
        }

        const auto [iterator, inserted] = values.emplace(
            std::string(name), std::string(value)
        );
        static_cast<void>(iterator);
        if (!inserted) {
            option_error(name, "duplicate option");
        }
    }
    return ParsedOptions(std::move(values));
}

bool ParsedOptions::contains(const std::string_view name) const {
    return values_.contains(std::string(name));
}

std::string_view ParsedOptions::require(const std::string_view name) const {
    const auto iterator = values_.find(std::string(name));
    if (iterator == values_.end()) {
        option_error(name, "required option is missing");
    }
    return iterator->second;
}

std::optional<std::string_view> ParsedOptions::optional(
    const std::string_view name
) const {
    const auto iterator = values_.find(std::string(name));
    if (iterator == values_.end()) {
        return std::nullopt;
    }
    return iterator->second;
}

bool is_help_request(
    const int argument_count,
    char* arguments[]
) {
    return argument_count == 2 && arguments != nullptr
        && std::string_view(arguments[1]) == "--help";
}

void initialize_result_channels() {
    struct sigaction action {};
    action.sa_handler = SIG_IGN;
    if (::sigemptyset(&action.sa_mask) == -1) {
        throw std::system_error(
            errno == 0 ? EIO : errno,
            std::generic_category(),
            "initialize SIGPIPE mask"
        );
    }
    errno = 0;
    if (::sigaction(SIGPIPE, &action, nullptr) == -1) {
        throw std::system_error(
            errno == 0 ? EIO : errno,
            std::generic_category(),
            "ignore SIGPIPE for CLI result channels"
        );
    }
}

std::uint64_t parse_u64(
    const std::string_view option,
    const std::string_view value
) {
    if (value.empty()) {
        option_error(option, "empty unsigned integer");
    }
    if (value.size() > 1U && value.front() == '0') {
        option_error(option, "unsigned integer has a leading zero");
    }
    if (!std::all_of(value.begin(), value.end(), [](const char character) {
            return character >= '0' && character <= '9';
        })) {
        option_error(option, "expected canonical unsigned decimal integer");
    }

    std::uint64_t result = 0U;
    const auto parsed = std::from_chars(
        value.data(), value.data() + value.size(), result, 10
    );
    if (parsed.ec == std::errc::result_out_of_range) {
        option_error(option, "unsigned integer is outside uint64 range");
    }
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
        option_error(option, "invalid unsigned integer");
    }
    return result;
}

std::size_t parse_size(
    const std::string_view option,
    const std::string_view value
) {
    const std::uint64_t parsed = parse_u64(option, value);
    if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
        if (parsed > std::numeric_limits<std::size_t>::max()) {
            option_error(option, "value is outside size_t range");
        }
    }
    return static_cast<std::size_t>(parsed);
}

double parse_finite_double(
    const std::string_view option,
    const std::string_view value
) {
    if (!is_canonical_decimal_float(value)) {
        option_error(option, "expected canonical decimal floating-point value");
    }
    double result = 0.0;
    const auto parsed = std::from_chars(
        value.data(),
        value.data() + value.size(),
        result,
        std::chars_format::general
    );
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()
        || !std::isfinite(result)) {
        option_error(option, "expected one finite decimal floating-point value");
    }
    return result;
}

void require_valid_utf8(
    const std::string_view option,
    const std::string_view value
) {
    for (std::size_t offset = 0U; offset < value.size();) {
        const auto byte = static_cast<unsigned char>(value[offset]);
        if (byte < 0x80U) {
            ++offset;
            continue;
        }
        const std::size_t sequence = valid_utf8_sequence_length(value, offset);
        if (sequence == 0U) {
            option_error(option, "value is not valid UTF-8");
        }
        offset += sequence;
    }
}

std::string json_quote(const std::string_view value) {
    std::string result;
    result.reserve(value.size() + 2U);
    result.push_back('"');
    for (std::size_t offset = 0U; offset < value.size();) {
        const auto character = static_cast<unsigned char>(value[offset]);
        switch (character) {
            case '"':
                result += "\\\"";
                break;
            case '\\':
                result += "\\\\";
                break;
            case '\b':
                result += "\\b";
                break;
            case '\f':
                result += "\\f";
                break;
            case '\n':
                result += "\\n";
                break;
            case '\r':
                result += "\\r";
                break;
            case '\t':
                result += "\\t";
                break;
            default:
                if (character >= 0x20U && character <= 0x7eU) {
                    result.push_back(static_cast<char>(character));
                } else if (character >= 0x80U) {
                    const std::size_t sequence = valid_utf8_sequence_length(
                        value, offset
                    );
                    if (sequence != 0U) {
                        result.append(value.substr(offset, sequence));
                        offset += sequence;
                        continue;
                    }
                    result += "\\u00";
                    result.push_back(hexadecimal_digit(character >> 4U));
                    result.push_back(hexadecimal_digit(character));
                } else {
                    result += "\\u00";
                    result.push_back(hexadecimal_digit(character >> 4U));
                    result.push_back(hexadecimal_digit(character));
                }
                break;
        }
        ++offset;
    }
    result.push_back('"');
    return result;
}

std::string json_number(const double value) {
    if (!std::isfinite(value)) {
        return "null";
    }
    char buffer[64U]{};
    const auto encoded = std::to_chars(
        std::begin(buffer),
        std::end(buffer),
        value,
        std::chars_format::general,
        std::numeric_limits<double>::max_digits10
    );
    if (encoded.ec != std::errc{}) {
        throw std::runtime_error("could not encode finite double as JSON");
    }
    return {buffer, encoded.ptr};
}

std::string json_hex_double(const double value) {
    if (std::isnan(value)) {
        return json_quote("nan");
    }
    if (std::isinf(value)) {
        return json_quote(std::signbit(value) ? "-inf" : "inf");
    }

    char buffer[64U]{};
    const auto encoded = std::to_chars(
        buffer,
        buffer + sizeof(buffer),
        value,
        std::chars_format::hex
    );
    if (encoded.ec != std::errc{}) {
        throw std::runtime_error("could not encode double as hexadecimal");
    }
    const std::string_view token(
        buffer, static_cast<std::size_t>(encoded.ptr - buffer)
    );
    std::string prefixed;
    if (token.starts_with('-')) {
        prefixed = "-0x" + std::string(token.substr(1U));
    } else {
        prefixed = "0x" + std::string(token);
    }
    return json_quote(prefixed);
}

void write_error_json(
    const std::string_view command,
    const std::string_view kind,
    const std::string_view code,
    const std::string_view message
) noexcept {
    try {
        std::string payload =
            "{\"schema\":\"TBANK_CLI_ERROR_V1\",\"command\":"
            + json_quote(command)
            + ",\"kind\":" + json_quote(kind)
            + ",\"code\":" + json_quote(code)
            + ",\"message\":" + json_quote(message) + "}\n";
        write_all(STDERR_FILENO, payload);
    } catch (...) {
        // The requested error exit remains truthful even when the diagnostic
        // channel itself is unavailable. There is no second recovery channel.
    }
}

void write_stdout(const std::string_view payload) {
    write_all(STDOUT_FILENO, payload);
}

void write_stderr(const std::string_view payload) noexcept {
    try {
        write_all(STDERR_FILENO, payload);
    } catch (...) {
    }
}

}  // namespace tbank::cli

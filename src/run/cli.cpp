#include "tbank/run/cli.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <limits>
#include <string>
#include <system_error>

#include <cerrno>
#include <signal.h>
#include <unistd.h>

namespace tbank::cli {
namespace {

[[noreturn]] void option_error(
    const std::string_view option,
    const std::string_view detail
) {
    throw UsageError(
        "option --" + std::string(option) + ": " + std::string(detail)
    );
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

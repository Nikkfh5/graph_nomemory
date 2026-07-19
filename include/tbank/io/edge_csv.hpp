#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>

namespace tbank::io {

struct ParsedEdge {
    std::int32_t source;
    std::int32_t destination;
    // 1-based physical input line. LF and CRLF each advance this by one.
    std::uint64_t input_line;
};

struct EdgeCsvSummary {
    std::uint64_t data_rows;
};

class EdgeCsvError final : public std::runtime_error {
public:
    // Parser errors carry the physical line; headers use line 1, header-only input line 2, and data
    // EOF the current line.
    EdgeCsvError(std::uint64_t input_line, const char* message);

    [[nodiscard]] std::uint64_t input_line() const noexcept;

private:
    std::uint64_t input_line_;
};

using EdgeConsumer = std::function<void(ParsedEdge)>;

// Strict bounded parser for int32 from,to CSV. Chunk boundaries are invisible; delivery is
// synchronous, and any error poisons the parser.
class EdgeCsvParser final {
public:
    explicit EdgeCsvParser(EdgeConsumer consumer);

    // Destruction abandons an unfinished stream and performs no implicit
    // validation. A caller may accept input only after one successful finish().
    ~EdgeCsvParser();

    EdgeCsvParser(EdgeCsvParser&&) = delete;
    EdgeCsvParser& operator=(EdgeCsvParser&&) = delete;
    EdgeCsvParser(const EdgeCsvParser&) = delete;
    EdgeCsvParser& operator=(const EdgeCsvParser&) = delete;

    // Input bytes are never retained; terminal parsers reject later calls.
    void consume(std::span<const char> bytes);

    // Finalizes an optional unterminated row and makes the parser terminal.
    [[nodiscard]] EdgeCsvSummary finish();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::io

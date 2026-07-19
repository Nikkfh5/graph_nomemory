#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace tbank::checksum {

inline constexpr std::size_t kSha256DigestBytes = 32U;
inline constexpr std::size_t kSha256HexCharacters = 64U;

using Sha256Digest = std::array<std::byte, kSha256DigestBytes>;

class Sha256Error final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Small stateful wrapper around the non-deprecated OpenSSL EVP interface.
// The context is ordinary runtime state covered by the caller's non-bulk
// reserve; update() never retains caller bytes.
class Sha256Hasher final {
public:
    Sha256Hasher();
    ~Sha256Hasher();

    Sha256Hasher(const Sha256Hasher&) = delete;
    Sha256Hasher& operator=(const Sha256Hasher&) = delete;
    Sha256Hasher(Sha256Hasher&&) noexcept;
    Sha256Hasher& operator=(Sha256Hasher&&) noexcept;

    void update(std::span<const std::byte> bytes);
    [[nodiscard]] Sha256Digest finish();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::string sha256_lower_hex(const Sha256Digest& digest);
[[nodiscard]] Sha256Digest parse_sha256_lower_hex(std::string_view value);

}  // namespace tbank::checksum

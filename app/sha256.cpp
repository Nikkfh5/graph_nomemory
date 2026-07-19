#include "sha256.hpp"

#include <algorithm>
#include <cstdint>
#include <utility>

#include <openssl/evp.h>

namespace tbank::checksum {
namespace {

[[nodiscard]] char hexadecimal_digit(const unsigned int value) noexcept {
    constexpr std::string_view digits = "0123456789abcdef";
    return digits[value & 0x0fU];
}

[[nodiscard]] unsigned int parse_hexadecimal_digit(const char value) {
    if (value >= '0' && value <= '9') {
        return static_cast<unsigned int>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
        return 10U + static_cast<unsigned int>(value - 'a');
    }
    throw std::invalid_argument(
        "SHA-256 must contain only lowercase hexadecimal characters"
    );
}

}  // namespace

class Sha256Hasher::Impl final {
public:
    Impl() : context_(EVP_MD_CTX_new()) {
        if (context_ == nullptr) {
            throw Sha256Error("OpenSSL could not allocate SHA-256 context");
        }
        if (EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1) {
            EVP_MD_CTX_free(context_);
            context_ = nullptr;
            throw Sha256Error("OpenSSL could not initialize SHA-256");
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    ~Impl() {
        EVP_MD_CTX_free(context_);
    }

    void update(const std::span<const std::byte> bytes) {
        require_active();
        if (!bytes.empty()
            && EVP_DigestUpdate(context_, bytes.data(), bytes.size()) != 1) {
            throw Sha256Error("OpenSSL SHA-256 update failed");
        }
    }

    [[nodiscard]] Sha256Digest finish() {
        require_active();
        Sha256Digest digest{};
        unsigned int digest_bytes = 0U;
        if (EVP_DigestFinal_ex(
                context_,
                reinterpret_cast<unsigned char*>(digest.data()),
                &digest_bytes
            ) != 1) {
            throw Sha256Error("OpenSSL SHA-256 finalization failed");
        }
        finished_ = true;
        if (digest_bytes != digest.size()) {
            throw Sha256Error("OpenSSL returned an invalid SHA-256 length");
        }
        return digest;
    }

private:
    void require_active() const {
        if (finished_) {
            throw std::logic_error("SHA-256 hasher is already finalized");
        }
        if (context_ == nullptr) {
            throw std::logic_error("SHA-256 hasher was moved from");
        }
    }

    EVP_MD_CTX* context_ = nullptr;
    bool finished_ = false;
};

Sha256Hasher::Sha256Hasher() : impl_(std::make_unique<Impl>()) {}
Sha256Hasher::~Sha256Hasher() = default;
Sha256Hasher::Sha256Hasher(Sha256Hasher&&) noexcept = default;
Sha256Hasher& Sha256Hasher::operator=(Sha256Hasher&&) noexcept = default;

void Sha256Hasher::update(const std::span<const std::byte> bytes) {
    if (impl_ == nullptr) {
        throw std::logic_error("SHA-256 hasher was moved from");
    }
    impl_->update(bytes);
}

Sha256Digest Sha256Hasher::finish() {
    if (impl_ == nullptr) {
        throw std::logic_error("SHA-256 hasher was moved from");
    }
    return impl_->finish();
}

std::string sha256_lower_hex(const Sha256Digest& digest) {
    std::string result;
    result.reserve(kSha256HexCharacters);
    for (const std::byte value : digest) {
        const unsigned int byte = std::to_integer<unsigned int>(value);
        result.push_back(hexadecimal_digit(byte >> 4U));
        result.push_back(hexadecimal_digit(byte));
    }
    return result;
}

Sha256Digest parse_sha256_lower_hex(const std::string_view value) {
    if (value.size() != kSha256HexCharacters) {
        throw std::invalid_argument(
            "SHA-256 must contain exactly 64 lowercase hexadecimal characters"
        );
    }
    Sha256Digest result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        const unsigned int high = parse_hexadecimal_digit(value[index * 2U]);
        const unsigned int low = parse_hexadecimal_digit(
            value[index * 2U + 1U]
        );
        result[index] = static_cast<std::byte>((high << 4U) | low);
    }
    return result;
}

}  // namespace tbank::checksum

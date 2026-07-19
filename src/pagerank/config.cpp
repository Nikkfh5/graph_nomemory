#include "tbank/pagerank/config.hpp"

#include <cmath>
#include <stdexcept>

namespace tbank::pagerank {

void validate_page_rank_config(const PageRankConfig& config) {
    if (!std::isfinite(config.alpha)
        || config.alpha <= 0.0 || config.alpha >= 1.0) {
        throw std::invalid_argument(
            "PageRank alpha must be finite and in (0, 1)"
        );
    }
    if (!std::isfinite(config.eta) || config.eta <= 0.0) {
        throw std::invalid_argument(
            "PageRank eta must be finite and positive"
        );
    }
    if (config.max_iterations == 0U) {
        throw std::invalid_argument(
            "PageRank max_iterations must be positive"
        );
    }
    if (!std::isfinite(config.tau_mass) || config.tau_mass < 0.0) {
        throw std::invalid_argument(
            "PageRank tau_mass must be finite and nonnegative"
        );
    }
    if (!std::isfinite(config.epsilon_verify)
        || config.epsilon_verify < 0.0) {
        throw std::invalid_argument(
            "PageRank epsilon_verify must be finite and nonnegative"
        );
    }
}

}  // namespace tbank::pagerank

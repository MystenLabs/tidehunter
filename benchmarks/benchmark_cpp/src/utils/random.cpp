#include "utils/random.h"
#include <algorithm>
#include <cmath>

namespace benchmark {

RandomGenerator::RandomGenerator(uint64_t seed) : rng_(seed) {}

std::vector<uint8_t> RandomGenerator::generate_bytes(size_t length) {
    std::vector<uint8_t> bytes(length);
    for (size_t i = 0; i < length; ++i) {
        bytes[i] = byte_dist_(rng_);
    }
    return bytes;
}

std::string RandomGenerator::generate_string(size_t length) {
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result.push_back(static_cast<char>(byte_dist_(rng_)));
    }
    return result;
}

std::string RandomGenerator::generate_key(size_t key_len) {
    return generate_string(key_len);
}

uint64_t RandomGenerator::next_u64() {
    return rng_();
}

uint64_t RandomGenerator::next_u64_range(uint64_t min, uint64_t max) {
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(rng_);
}

double RandomGenerator::next_f64() {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng_);
}

// Zipf distribution implementation
double ZipfDistribution::zeta(size_t n, double theta) {
    double sum = 0.0;
    for (size_t i = 1; i <= n; ++i) {
        sum += 1.0 / std::pow(i, theta);
    }
    return sum;
}

ZipfDistribution::ZipfDistribution(size_t n, double theta, uint64_t seed)
    : rng_(seed), n_(n), theta_(theta), zeta_n_(zeta(n, theta)) {

    // Precompute powers for efficiency
    powers_.reserve(n + 1);
    powers_.push_back(0.0); // powers_[0] unused
    for (size_t i = 1; i <= n; ++i) {
        powers_.push_back(1.0 / std::pow(i, theta));
    }
}

size_t ZipfDistribution::next() {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double u = dist(rng_) * zeta_n_;

    double sum = 0.0;
    for (size_t i = 1; i <= n_; ++i) {
        sum += powers_[i];
        if (sum >= u) {
            return i - 1; // Return 0-indexed
        }
    }
    return n_ - 1;
}

// Choice distribution implementation
ChoiceDistribution::ChoiceDistribution(size_t num_choices, uint64_t seed)
    : rng_(seed), dist_(0, num_choices - 1) {}

size_t ChoiceDistribution::next() {
    return dist_(rng_);
}

} // namespace benchmark
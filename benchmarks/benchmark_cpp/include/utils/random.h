#pragma once

#include <random>
#include <vector>
#include <string>
#include <cstddef>

namespace benchmark {

class RandomGenerator {
private:
    std::mt19937_64 rng_;
    std::uniform_int_distribution<uint8_t> byte_dist_{0, 255};

public:
    explicit RandomGenerator(uint64_t seed = std::random_device{}());

    // Generate random bytes
    std::vector<uint8_t> generate_bytes(size_t length);
    std::string generate_string(size_t length);

    // Generate random key
    std::string generate_key(size_t key_len);

    // Random number generation
    uint64_t next_u64();
    uint64_t next_u64_range(uint64_t min, uint64_t max);
    double next_f64();
};

// Zipf distribution for skewed access patterns
class ZipfDistribution {
private:
    std::mt19937_64 rng_;
    size_t n_;
    double theta_;
    double zeta_n_;
    std::vector<double> powers_;

    double zeta(size_t n, double theta);

public:
    ZipfDistribution(size_t n, double theta, uint64_t seed = std::random_device{}());

    size_t next();
};

// Choice distribution for selecting from a small set
class ChoiceDistribution {
private:
    std::mt19937_64 rng_;
    std::uniform_int_distribution<size_t> dist_;

public:
    ChoiceDistribution(size_t num_choices, uint64_t seed = std::random_device{}());

    size_t next();
};

} // namespace benchmark
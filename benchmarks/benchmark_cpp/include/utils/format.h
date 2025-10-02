#pragma once

#include <string>
#include <chrono>
#include <cstddef>

namespace benchmark {

// Format bytes as human-readable size (e.g., "1.5 MB")
std::string format_bytes(size_t bytes);

// Format number with thousands separators (e.g., "1,000,000")
std::string format_number(uint64_t num);

// Format duration as human-readable time (e.g., "2m 30s")
std::string format_duration(std::chrono::milliseconds ms);

// Format throughput as ops/sec with appropriate units
std::string format_throughput(double ops_per_sec);

// Format latency with appropriate units (ns, µs, ms, s)
std::string format_latency(double nanoseconds);

// Format number with decimal K/M suffix matching Rust (e.g., "102.00K")
std::string format_dec_div(uint64_t n);

// Format bytes with Kb/Mb suffix matching Rust (e.g., "99Mb")
std::string format_byte_div(uint64_t n);

// Get current timestamp in milliseconds for report prefix
uint64_t get_timestamp_ms();

// Progress bar formatter
class ProgressBar {
private:
    size_t total_;
    size_t current_;
    size_t width_;
    std::chrono::steady_clock::time_point start_time_;

public:
    explicit ProgressBar(size_t total, size_t width = 50);

    void update(size_t current);
    void increment(size_t delta = 1);
    void finish();
    std::string render() const;
};

} // namespace benchmark
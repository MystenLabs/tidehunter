#pragma once

#include <atomic>
#include <vector>
#include <mutex>
#include <string>
#include <chrono>
#include <map>

namespace benchmark {

// Simple histogram for latency tracking
class LatencyHistogram {
private:
    std::vector<std::atomic<uint64_t>> buckets_;
    std::atomic<uint64_t> total_count_{0};
    std::atomic<uint64_t> total_sum_{0};

    // Bucket boundaries in nanoseconds
    static constexpr uint64_t BUCKET_BOUNDARIES[] = {
        100,        // 100 ns
        200,        // 200 ns
        500,        // 500 ns
        1000,       // 1 µs
        2000,       // 2 µs
        5000,       // 5 µs
        10000,      // 10 µs
        20000,      // 20 µs
        50000,      // 50 µs
        100000,     // 100 µs
        200000,     // 200 µs
        500000,     // 500 µs
        1000000,    // 1 ms
        2000000,    // 2 ms
        5000000,    // 5 ms
        10000000,   // 10 ms
        20000000,   // 20 ms
        50000000,   // 50 ms
        100000000,  // 100 ms
        200000000,  // 200 ms
        500000000,  // 500 ms
        1000000000  // 1 s
    };
    static constexpr size_t NUM_BUCKETS = sizeof(BUCKET_BOUNDARIES) / sizeof(BUCKET_BOUNDARIES[0]) + 1;

    size_t get_bucket_index(uint64_t value_ns) const;

public:
    LatencyHistogram();

    void record(uint64_t latency_ns);
    void record(std::chrono::nanoseconds latency);

    double mean() const;
    uint64_t percentile(double p) const;
    uint64_t min() const;
    uint64_t max() const;
    uint64_t count() const;

    void reset();
    void print_summary(const std::string& label) const;

    // Print single-line format matching Rust benchmark
    // Format: "Latency(mcs): p50: X..=X, p90: Y..=Y, ..."
    std::string format_latency_line() const;
};

// Metrics collection for the benchmark
class Metrics {
private:
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point end_time_;

    std::atomic<uint64_t> write_ops_{0};
    std::atomic<uint64_t> read_ops_{0};
    std::atomic<uint64_t> bytes_written_{0};
    std::atomic<uint64_t> bytes_read_{0};

    LatencyHistogram write_latency_;
    LatencyHistogram read_latency_;

    mutable std::mutex summary_mutex_;

public:
    Metrics();

    void start();
    void stop();

    void record_write(uint64_t bytes, std::chrono::nanoseconds latency);
    void record_read(uint64_t bytes, std::chrono::nanoseconds latency);

    void increment_write_ops(uint64_t count = 1);
    void increment_read_ops(uint64_t count = 1);
    void add_bytes_written(uint64_t bytes);
    void add_bytes_read(uint64_t bytes);

    double elapsed_seconds() const;
    double write_throughput() const;
    double read_throughput() const;
    double write_bandwidth_mb_s() const;
    double read_bandwidth_mb_s() const;

    uint64_t total_write_ops() const { return write_ops_.load(); }
    uint64_t total_read_ops() const { return read_ops_.load(); }
    uint64_t total_bytes_written() const { return bytes_written_.load(); }
    uint64_t total_bytes_read() const { return bytes_read_.load(); }

    const LatencyHistogram& write_latency() const { return write_latency_; }
    const LatencyHistogram& read_latency() const { return read_latency_; }
    LatencyHistogram& write_latency() { return write_latency_; }
    LatencyHistogram& read_latency() { return read_latency_; }

    void print_summary() const;
    void reset();
};

// Thread-local metrics aggregator
class ThreadLocalMetrics {
private:
    struct LocalMetrics {
        uint64_t write_ops = 0;
        uint64_t read_ops = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_read = 0;
    };

    static thread_local LocalMetrics local_;
    static std::mutex global_mutex_;
    static std::vector<LocalMetrics*> all_locals_;

    Metrics* global_metrics_;

public:
    explicit ThreadLocalMetrics(Metrics* global);
    ~ThreadLocalMetrics();

    void record_write(uint64_t bytes, std::chrono::nanoseconds latency);
    void record_read(uint64_t bytes, std::chrono::nanoseconds latency);

    void flush();
    static void flush_all(Metrics* global);
};

} // namespace benchmark
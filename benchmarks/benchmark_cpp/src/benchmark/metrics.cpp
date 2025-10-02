#include "benchmark/metrics.h"
#include "utils/format.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <sstream>

namespace benchmark {

// LatencyHistogram implementation
LatencyHistogram::LatencyHistogram() : buckets_(NUM_BUCKETS) {
    for (auto& bucket : buckets_) {
        bucket.store(0);
    }
}

size_t LatencyHistogram::get_bucket_index(uint64_t value_ns) const {
    for (size_t i = 0; i < NUM_BUCKETS - 1; ++i) {
        if (value_ns <= BUCKET_BOUNDARIES[i]) {
            return i;
        }
    }
    return NUM_BUCKETS - 1;
}

void LatencyHistogram::record(uint64_t latency_ns) {
    size_t bucket = get_bucket_index(latency_ns);
    buckets_[bucket].fetch_add(1);
    total_count_.fetch_add(1);
    total_sum_.fetch_add(latency_ns);
}

void LatencyHistogram::record(std::chrono::nanoseconds latency) {
    record(static_cast<uint64_t>(latency.count()));
}

double LatencyHistogram::mean() const {
    uint64_t count = total_count_.load();
    if (count == 0) return 0.0;
    return static_cast<double>(total_sum_.load()) / count;
}

uint64_t LatencyHistogram::percentile(double p) const {
    uint64_t count = total_count_.load();
    if (count == 0) return 0;

    uint64_t target = static_cast<uint64_t>(count * p / 100.0);
    uint64_t running_count = 0;

    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        running_count += buckets_[i].load();
        if (running_count >= target) {
            if (i == 0) {
                return BUCKET_BOUNDARIES[0] / 2;
            } else if (i == NUM_BUCKETS - 1) {
                return BUCKET_BOUNDARIES[NUM_BUCKETS - 2];
            } else {
                // Return middle of bucket
                return (BUCKET_BOUNDARIES[i-1] + BUCKET_BOUNDARIES[i]) / 2;
            }
        }
    }
    return BUCKET_BOUNDARIES[NUM_BUCKETS - 2];
}

uint64_t LatencyHistogram::min() const {
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        if (buckets_[i].load() > 0) {
            if (i == 0) {
                return 0;
            } else {
                return BUCKET_BOUNDARIES[i-1];
            }
        }
    }
    return 0;
}

uint64_t LatencyHistogram::max() const {
    for (size_t i = NUM_BUCKETS - 1; i > 0; --i) {
        if (buckets_[i].load() > 0) {
            if (i == NUM_BUCKETS - 1) {
                return BUCKET_BOUNDARIES[NUM_BUCKETS - 2];
            } else {
                return BUCKET_BOUNDARIES[i];
            }
        }
    }
    return 0;
}

uint64_t LatencyHistogram::count() const {
    return total_count_.load();
}

void LatencyHistogram::reset() {
    for (auto& bucket : buckets_) {
        bucket.store(0);
    }
    total_count_.store(0);
    total_sum_.store(0);
}

void LatencyHistogram::print_summary(const std::string& label) const {
    std::cout << label << " Latency Statistics:" << std::endl;
    std::cout << "  Count: " << format_number(count()) << std::endl;
    std::cout << "  Mean: " << format_latency(mean()) << std::endl;
    std::cout << "  Min: " << format_latency(min()) << std::endl;
    std::cout << "  P50: " << format_latency(percentile(50)) << std::endl;
    std::cout << "  P90: " << format_latency(percentile(90)) << std::endl;
    std::cout << "  P99: " << format_latency(percentile(99)) << std::endl;
    std::cout << "  P99.9: " << format_latency(percentile(99.9)) << std::endl;
    std::cout << "  Max: " << format_latency(max()) << std::endl;
}

std::string LatencyHistogram::format_latency_line() const {
    // Convert nanoseconds to microseconds (rounded)
    auto to_mcs = [](uint64_t ns) -> uint64_t {
        return (ns + 500) / 1000;  // Round to nearest microsecond
    };

    uint64_t p50 = to_mcs(percentile(50));
    uint64_t p90 = to_mcs(percentile(90));
    uint64_t p99 = to_mcs(percentile(99));
    uint64_t p999 = to_mcs(percentile(99.9));
    uint64_t p9999 = to_mcs(percentile(99.99));
    uint64_t p99999 = to_mcs(percentile(99.999));

    std::stringstream ss;
    ss << "Latency(mcs): "
       << "p50: " << p50 << "..=" << p50 << ", "
       << "p90: " << p90 << "..=" << p90 << ", "
       << "p99: " << p99 << "..=" << p99 << ", "
       << "p99.9: " << p999 << "..=" << p999 << ", "
       << "p99.99: " << p9999 << "..=" << p9999 << ", "
       << "p99.999: " << p99999 << "..=" << p99999;

    return ss.str();
}

// Metrics implementation
Metrics::Metrics() : start_time_(std::chrono::steady_clock::now()) {}

void Metrics::start() {
    start_time_ = std::chrono::steady_clock::now();
}

void Metrics::stop() {
    end_time_ = std::chrono::steady_clock::now();
}

void Metrics::record_write(uint64_t bytes, std::chrono::nanoseconds latency) {
    write_ops_.fetch_add(1);
    bytes_written_.fetch_add(bytes);
    write_latency_.record(latency);
}

void Metrics::record_read(uint64_t bytes, std::chrono::nanoseconds latency) {
    read_ops_.fetch_add(1);
    bytes_read_.fetch_add(bytes);
    read_latency_.record(latency);
}

void Metrics::increment_write_ops(uint64_t count) {
    write_ops_.fetch_add(count);
}

void Metrics::increment_read_ops(uint64_t count) {
    read_ops_.fetch_add(count);
}

void Metrics::add_bytes_written(uint64_t bytes) {
    bytes_written_.fetch_add(bytes);
}

void Metrics::add_bytes_read(uint64_t bytes) {
    bytes_read_.fetch_add(bytes);
}

double Metrics::elapsed_seconds() const {
    auto end = (end_time_ == std::chrono::steady_clock::time_point{})
        ? std::chrono::steady_clock::now()
        : end_time_;
    auto duration = end - start_time_;
    return std::chrono::duration<double>(duration).count();
}

double Metrics::write_throughput() const {
    double elapsed = elapsed_seconds();
    if (elapsed == 0) return 0;
    return write_ops_.load() / elapsed;
}

double Metrics::read_throughput() const {
    double elapsed = elapsed_seconds();
    if (elapsed == 0) return 0;
    return read_ops_.load() / elapsed;
}

double Metrics::write_bandwidth_mb_s() const {
    double elapsed = elapsed_seconds();
    if (elapsed == 0) return 0;
    return (bytes_written_.load() / (1024.0 * 1024.0)) / elapsed;
}

double Metrics::read_bandwidth_mb_s() const {
    double elapsed = elapsed_seconds();
    if (elapsed == 0) return 0;
    return (bytes_read_.load() / (1024.0 * 1024.0)) / elapsed;
}

void Metrics::print_summary() const {
    std::lock_guard<std::mutex> lock(summary_mutex_);

    std::cout << "\n=== Benchmark Results ===" << std::endl;
    std::cout << "Duration: " << format_duration(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double>(elapsed_seconds()))) << std::endl;

    if (write_ops_.load() > 0) {
        std::cout << "\nWrite Operations:" << std::endl;
        std::cout << "  Total: " << format_number(write_ops_.load()) << std::endl;
        std::cout << "  Throughput: " << format_throughput(write_throughput()) << std::endl;
        std::cout << "  Bandwidth: " << std::fixed << std::setprecision(2)
                  << write_bandwidth_mb_s() << " MB/s" << std::endl;
        write_latency_.print_summary("Write");
    }

    if (read_ops_.load() > 0) {
        std::cout << "\nRead Operations:" << std::endl;
        std::cout << "  Total: " << format_number(read_ops_.load()) << std::endl;
        std::cout << "  Throughput: " << format_throughput(read_throughput()) << std::endl;
        std::cout << "  Bandwidth: " << std::fixed << std::setprecision(2)
                  << read_bandwidth_mb_s() << " MB/s" << std::endl;
        read_latency_.print_summary("Read");
    }
}

void Metrics::reset() {
    start_time_ = std::chrono::steady_clock::now();
    end_time_ = {};
    write_ops_.store(0);
    read_ops_.store(0);
    bytes_written_.store(0);
    bytes_read_.store(0);
    write_latency_.reset();
    read_latency_.reset();
}

// ThreadLocalMetrics implementation
thread_local ThreadLocalMetrics::LocalMetrics ThreadLocalMetrics::local_;
std::mutex ThreadLocalMetrics::global_mutex_;
std::vector<ThreadLocalMetrics::LocalMetrics*> ThreadLocalMetrics::all_locals_;

ThreadLocalMetrics::ThreadLocalMetrics(Metrics* global) : global_metrics_(global) {
    std::lock_guard<std::mutex> lock(global_mutex_);
    all_locals_.push_back(&local_);
}

ThreadLocalMetrics::~ThreadLocalMetrics() {
    flush();
    std::lock_guard<std::mutex> lock(global_mutex_);
    all_locals_.erase(std::remove(all_locals_.begin(), all_locals_.end(), &local_),
                      all_locals_.end());
}

void ThreadLocalMetrics::record_write(uint64_t bytes, std::chrono::nanoseconds latency) {
    local_.write_ops++;
    local_.bytes_written += bytes;
    global_metrics_->write_latency().record(latency);

    // Periodically flush to global
    if (local_.write_ops % 1000 == 0) {
        flush();
    }
}

void ThreadLocalMetrics::record_read(uint64_t bytes, std::chrono::nanoseconds latency) {
    local_.read_ops++;
    local_.bytes_read += bytes;
    global_metrics_->read_latency().record(latency);

    // Periodically flush to global
    if (local_.read_ops % 1000 == 0) {
        flush();
    }
}

void ThreadLocalMetrics::flush() {
    if (local_.write_ops > 0) {
        global_metrics_->increment_write_ops(local_.write_ops);
        global_metrics_->add_bytes_written(local_.bytes_written);
        local_.write_ops = 0;
        local_.bytes_written = 0;
    }

    if (local_.read_ops > 0) {
        global_metrics_->increment_read_ops(local_.read_ops);
        global_metrics_->add_bytes_read(local_.bytes_read);
        local_.read_ops = 0;
        local_.bytes_read = 0;
    }
}

void ThreadLocalMetrics::flush_all(Metrics* global) {
    std::lock_guard<std::mutex> lock(global_mutex_);
    for (auto* local : all_locals_) {
        if (local->write_ops > 0) {
            global->increment_write_ops(local->write_ops);
            global->add_bytes_written(local->bytes_written);
            local->write_ops = 0;
            local->bytes_written = 0;
        }

        if (local->read_ops > 0) {
            global->increment_read_ops(local->read_ops);
            global->add_bytes_read(local->bytes_read);
            local->read_ops = 0;
            local->bytes_read = 0;
        }
    }
}

} // namespace benchmark
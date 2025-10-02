#include "utils/format.h"
#include <sstream>
#include <iomanip>
#include <iostream>
#include <chrono>

namespace benchmark {

std::string format_bytes(size_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    double size = static_cast<double>(bytes);
    size_t unit_idx = 0;

    while (size >= 1024.0 && unit_idx < sizeof(units) / sizeof(units[0]) - 1) {
        size /= 1024.0;
        unit_idx++;
    }

    std::ostringstream oss;
    if (unit_idx == 0) {
        oss << bytes << " " << units[unit_idx];
    } else {
        oss << std::fixed << std::setprecision(2) << size << " " << units[unit_idx];
    }
    return oss.str();
}

std::string format_number(uint64_t num) {
    std::string str = std::to_string(num);
    std::string result;

    int count = 0;
    for (auto it = str.rbegin(); it != str.rend(); ++it) {
        if (count > 0 && count % 3 == 0) {
            result = ',' + result;
        }
        result = *it + result;
        count++;
    }

    return result;
}

std::string format_duration(std::chrono::milliseconds ms) {
    auto total_seconds = ms.count() / 1000;
    auto hours = total_seconds / 3600;
    auto minutes = (total_seconds % 3600) / 60;
    auto seconds = total_seconds % 60;
    auto millis = ms.count() % 1000;

    std::ostringstream oss;
    if (hours > 0) {
        oss << hours << "h ";
    }
    if (minutes > 0 || hours > 0) {
        oss << minutes << "m ";
    }
    if (seconds > 0 || minutes > 0 || hours > 0) {
        oss << seconds << "s";
    }
    if (hours == 0 && minutes == 0 && seconds == 0) {
        oss << millis << "ms";
    }

    return oss.str();
}

std::string format_throughput(double ops_per_sec) {
    std::ostringstream oss;
    if (ops_per_sec >= 1e9) {
        oss << std::fixed << std::setprecision(2) << ops_per_sec / 1e9 << " Gops/s";
    } else if (ops_per_sec >= 1e6) {
        oss << std::fixed << std::setprecision(2) << ops_per_sec / 1e6 << " Mops/s";
    } else if (ops_per_sec >= 1e3) {
        oss << std::fixed << std::setprecision(2) << ops_per_sec / 1e3 << " Kops/s";
    } else {
        oss << std::fixed << std::setprecision(2) << ops_per_sec << " ops/s";
    }
    return oss.str();
}

std::string format_latency(double nanoseconds) {
    std::ostringstream oss;
    if (nanoseconds >= 1e9) {
        oss << std::fixed << std::setprecision(2) << nanoseconds / 1e9 << " s";
    } else if (nanoseconds >= 1e6) {
        oss << std::fixed << std::setprecision(2) << nanoseconds / 1e6 << " ms";
    } else if (nanoseconds >= 1e3) {
        oss << std::fixed << std::setprecision(2) << nanoseconds / 1e3 << " µs";
    } else {
        oss << std::fixed << std::setprecision(0) << nanoseconds << " ns";
    }
    return oss.str();
}

ProgressBar::ProgressBar(size_t total, size_t width)
    : total_(total), current_(0), width_(width),
      start_time_(std::chrono::steady_clock::now()) {}

void ProgressBar::update(size_t current) {
    current_ = current;
    std::cout << "\r" << render() << std::flush;
}

void ProgressBar::increment(size_t delta) {
    update(current_ + delta);
}

void ProgressBar::finish() {
    update(total_);
    std::cout << std::endl;
}

std::string ProgressBar::render() const {
    double progress = static_cast<double>(current_) / total_;
    size_t filled = static_cast<size_t>(progress * width_);

    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < width_; ++i) {
        if (i < filled) {
            oss << "=";
        } else if (i == filled) {
            oss << ">";
        } else {
            oss << " ";
        }
    }
    oss << "] ";

    oss << std::fixed << std::setprecision(1) << (progress * 100) << "% ";
    oss << "(" << current_ << "/" << total_ << ")";

    // Add ETA if we have made progress
    if (current_ > 0 && current_ < total_) {
        auto elapsed = std::chrono::steady_clock::now() - start_time_;
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        auto estimated_total = elapsed_ms * total_ / current_;
        auto remaining = estimated_total - elapsed_ms;
        oss << " ETA: " << format_duration(remaining);
    }

    return oss.str();
}

// Format number with decimal K/M suffix matching Rust's dec_div
std::string format_dec_div(uint64_t n) {
    const uint64_t M = 1000000;
    const uint64_t K = 1000;

    std::ostringstream oss;
    if (n > M) {
        oss << std::fixed << std::setprecision(2) << (static_cast<double>(n) / M) << "M";
    } else if (n > K) {
        oss << std::fixed << std::setprecision(2) << (static_cast<double>(n) / K) << "K";
    } else {
        oss << n;
    }
    return oss.str();
}

// Format bytes with Kb/Mb suffix matching Rust's byte_div
std::string format_byte_div(uint64_t n) {
    const uint64_t K = 1024;
    const uint64_t M = K * K;

    std::ostringstream oss;
    if (n > M) {
        oss << (n / M) << "Mb";
    } else if (n > K) {
        oss << (n / K) << "Kb";
    } else {
        oss << n;
    }
    return oss.str();
}

// Get current timestamp in milliseconds
uint64_t get_timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    return static_cast<uint64_t>(ms);
}

} // namespace benchmark
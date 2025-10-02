#include "benchmark/stress.h"
#include "utils/random.h"
#include "utils/format.h"
#include "storage/storage_factory.h"
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <atomic>

namespace benchmark {

// Helper function to print with timestamp matching Rust format
static void report(const std::string& message) {
    uint64_t ms = get_timestamp_ms();
    std::cout << "[" << ms << "] " << message << std::endl;
}

// StressTest implementation
StressTest::StressTest(std::unique_ptr<Storage> storage, const StressConfig& config)
    : storage_(std::move(storage)), config_(config) {
}

void StressTest::run() {
    report("Starting write test");

    uint64_t initial_writes = 0;  // Track writes from write phase

    // Write phase
    if (config_.reuse.empty()) {
        auto write_start = std::chrono::steady_clock::now();
        run_write_phase();
        auto write_end = std::chrono::steady_clock::now();

        auto write_duration = write_end - write_start;
        auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(write_duration).count();
        double write_seconds = std::chrono::duration<double>(write_duration).count();
        initial_writes = written_keys_.size();
        uint64_t total_bytes = initial_writes * config_.write_size;

        // Print latency line first (matching Rust order)
        report(metrics_.write_latency().format_latency_line());

        // Print write test summary matching Rust format exactly
        std::stringstream ss;
        ss << "Write test done in " << write_ms << "ms: "
           << format_dec_div(static_cast<uint64_t>(initial_writes / write_seconds))
           << " writes/s, "
           << format_byte_div(static_cast<uint64_t>(total_bytes / write_seconds))
           << "/sec";
        report(ss.str());
    } else {
        report("Skipping writes because reuse is specified");
    }

    // Storage size reporting (matching Rust)
    try {
        std::filesystem::path storage_path(config_.path);
        if (std::filesystem::exists(storage_path)) {
            size_t storage_size = 0;
            for (const auto& entry : std::filesystem::recursive_directory_iterator(storage_path)) {
                if (entry.is_regular_file()) {
                    storage_size += entry.file_size();
                }
            }

            std::stringstream ss;
            ss << std::fixed << std::setprecision(1)
               << "Storage used " << (storage_size / (1024.0 * 1024.0 * 1024.0)) << " Gb";
            report(ss.str());
        }
    } catch (...) {
        // Ignore errors in size calculation
    }

    // Mixed read/write phase
    if (config_.operations > 0) {
        std::stringstream ss;
        ss << "Starting mixed read/write test ("
           << static_cast<int>(config_.read_percentage) << "% reads, "
           << (100 - config_.read_percentage) << "% writes)";
        report(ss.str());

        auto mixed_start = std::chrono::steady_clock::now();
        run_mixed_phase();
        auto mixed_end = std::chrono::steady_clock::now();

        auto mixed_duration = mixed_end - mixed_start;
        auto mixed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(mixed_duration).count();
        double mixed_seconds = std::chrono::duration<double>(mixed_duration).count();
        uint64_t total_ops = metrics_.total_read_ops() + metrics_.total_write_ops() - initial_writes;
        uint64_t total_bytes = (metrics_.total_bytes_read() + metrics_.total_bytes_written() -
                                initial_writes * config_.write_size);

        // Print latency line first (matching Rust order)
        if (metrics_.total_read_ops() > 0) {
            report(metrics_.read_latency().format_latency_line());
        }

        // Print mixed test summary matching Rust format exactly
        ss.str("");
        ss << "Mixed test done in " << mixed_ms << "ms: "
           << format_dec_div(static_cast<uint64_t>(total_ops / mixed_seconds))
           << " ops/s, "
           << format_byte_div(static_cast<uint64_t>(total_bytes / mixed_seconds))
           << "/sec";
        report(ss.str());
    }
}

void StressTest::run_write_phase() {
    std::vector<std::thread> threads;
    std::mutex key_mutex;

    size_t writes_per_thread = config_.writes / config_.write_threads;
    size_t remaining_writes = config_.writes % config_.write_threads;

    for (size_t thread_id = 0; thread_id < config_.write_threads; ++thread_id) {
        size_t thread_writes = writes_per_thread;
        if (thread_id < remaining_writes) {
            thread_writes++;
        }

        threads.emplace_back([this, thread_id, thread_writes, &key_mutex]() {
            RandomGenerator rng(thread_id);
            std::vector<std::string> local_keys;
            local_keys.reserve(thread_writes);

            for (size_t i = 0; i < thread_writes; ++i) {
                // Generate key based on layout
                std::string key = generate_key(thread_id, i, config_.key_len);
                std::string value = rng.generate_string(config_.write_size);

                auto start = std::chrono::high_resolution_clock::now();
                storage_->insert(key, value);
                auto end = std::chrono::high_resolution_clock::now();

                auto latency = end - start;
                metrics_.record_write(config_.write_size, latency);

                local_keys.push_back(key);
            }

            // Add local keys to global set
            std::lock_guard<std::mutex> lock(key_mutex);
            written_keys_.insert(written_keys_.end(), local_keys.begin(), local_keys.end());
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

void StressTest::run_mixed_phase() {
    std::vector<std::thread> threads;

    // Start background writer threads if configured
    std::atomic<bool> stop_background{false};
    std::vector<std::thread> background_threads;

    if (config_.background_writes > 0) {
        for (size_t i = 0; i < config_.write_threads; ++i) {
            background_threads.emplace_back([this, i, &stop_background]() {
                run_background_writes(i, stop_background);
            });
        }
    }

    // Run mixed workload threads
    size_t ops_per_thread = config_.operations / config_.mixed_threads;
    size_t remaining_ops = config_.operations % config_.mixed_threads;

    for (size_t thread_id = 0; thread_id < config_.mixed_threads; ++thread_id) {
        size_t thread_ops = ops_per_thread;
        if (thread_id < remaining_ops) {
            thread_ops++;
        }

        threads.emplace_back([this, thread_id, thread_ops]() {
            run_mixed_operations(thread_id, thread_ops);
        });
    }

    // Wait for mixed threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Stop background threads
    stop_background = true;
    for (auto& t : background_threads) {
        t.join();
    }
}

void StressTest::run_mixed_operations(size_t thread_id, size_t num_ops) {
    RandomGenerator rng(thread_id + 1000);  // Different seed from write phase

    // Create distribution for key selection based on layout
    std::unique_ptr<ZipfDistribution> zipf;
    std::unique_ptr<ChoiceDistribution> choice;

    if (config_.key_layout == StressConfig::KeyLayout::ZIPF) {
        zipf = std::make_unique<ZipfDistribution>(written_keys_.size(), 0.99, thread_id);
    } else if (config_.key_layout == StressConfig::KeyLayout::CHOICE_SEQUENCE ||
               config_.key_layout == StressConfig::KeyLayout::SEQUENCE_CHOICE) {
        choice = std::make_unique<ChoiceDistribution>(10, thread_id);
    }

    for (size_t op = 0; op < num_ops; ++op) {
        bool is_read = (rng.next_u64_range(0, 99) < config_.read_percentage);

        if (is_read && !written_keys_.empty()) {
            // Select key based on distribution
            size_t key_index;
            if (zipf) {
                key_index = zipf->next() % written_keys_.size();
            } else if (choice) {
                // For choice distributions, select from a subset
                size_t choice_index = choice->next();
                size_t segment_size = written_keys_.size() / 10;
                size_t segment_start = choice_index * segment_size;
                key_index = segment_start + (rng.next_u64() % segment_size);
            } else {
                // Uniform random selection
                key_index = rng.next_u64_range(0, written_keys_.size() - 1);
            }

            const std::string& key = written_keys_[key_index];

            auto start = std::chrono::high_resolution_clock::now();

            switch (config_.read_mode) {
                case StressConfig::ReadMode::GET: {
                    auto value = storage_->get(key);
                    auto end = std::chrono::high_resolution_clock::now();
                    if (value.has_value()) {
                        metrics_.record_read(value->size(), end - start);
                    }
                    break;
                }
                case StressConfig::ReadMode::LT: {
                    auto values = storage_->get_lt(key, config_.lt_iterations);
                    auto end = std::chrono::high_resolution_clock::now();
                    size_t total_bytes = 0;
                    for (const auto& v : values) {
                        total_bytes += v.size();
                    }
                    metrics_.record_read(total_bytes, end - start);
                    break;
                }
                case StressConfig::ReadMode::EXISTS: {
                    [[maybe_unused]] bool exists = storage_->exists(key);
                    auto end = std::chrono::high_resolution_clock::now();
                    metrics_.record_read(0, end - start);
                    break;
                }
            }
        } else {
            // Write operation
            std::string key = generate_key(thread_id + 1000, op, config_.key_len);
            std::string value = rng.generate_string(config_.write_size);

            auto start = std::chrono::high_resolution_clock::now();
            storage_->insert(key, value);
            auto end = std::chrono::high_resolution_clock::now();

            metrics_.record_write(config_.write_size, end - start);

            // Add to written keys for future reads
            if (is_read) {  // Only add if we were trying to read
                written_keys_.push_back(key);
            }
        }
    }
}

void StressTest::run_background_writes(size_t thread_id, std::atomic<bool>& stop) {
    RandomGenerator rng(thread_id + 2000);  // Different seed
    size_t writes_per_second = config_.background_writes / config_.write_threads;
    auto write_interval = std::chrono::microseconds(1000000 / writes_per_second);

    size_t counter = 0;
    auto next_write_time = std::chrono::steady_clock::now();

    while (!stop.load()) {
        next_write_time += write_interval;

        std::string key = "bg_" + generate_key(thread_id, counter++, config_.key_len - 3);
        std::string value = rng.generate_string(config_.write_size);

        auto start = std::chrono::high_resolution_clock::now();
        storage_->insert(key, value);
        auto end = std::chrono::high_resolution_clock::now();

        metrics_.record_write(config_.write_size, end - start);

        // Sleep until next write
        std::this_thread::sleep_until(next_write_time);
    }
}

std::string StressTest::generate_key(size_t thread_id, size_t sequence, size_t key_len) {
    std::string key(key_len, '\0');

    // Encode thread_id and sequence into key
    uint64_t combined = (static_cast<uint64_t>(thread_id) << 32) | sequence;

    switch (config_.key_layout) {
        case StressConfig::KeyLayout::UNIFORM:
        case StressConfig::KeyLayout::ZIPF:
            // Big-endian encoding of combined value
            for (size_t i = 0; i < 8 && i < key_len; ++i) {
                key[key_len - 1 - i] = (combined >> (i * 8)) & 0xFF;
            }
            break;

        case StressConfig::KeyLayout::SEQUENCE_CHOICE:
            // First 8 bytes: sequence
            for (size_t i = 0; i < 8 && i < key_len; ++i) {
                key[i] = (sequence >> (i * 8)) & 0xFF;
            }
            // Rest: deterministic based on thread_id
            for (size_t i = 8; i < key_len; ++i) {
                key[i] = ((thread_id * 31 + i) * 17) & 0xFF;
            }
            break;

        case StressConfig::KeyLayout::CHOICE_SEQUENCE:
            // First part: deterministic based on thread_id
            for (size_t i = 0; i < key_len - 8 && i < key_len; ++i) {
                key[i] = ((thread_id * 31 + i) * 17) & 0xFF;
            }
            // Last 8 bytes: sequence
            for (size_t i = 0; i < 8 && (key_len - 8 + i) < key_len; ++i) {
                key[key_len - 8 + i] = (sequence >> (i * 8)) & 0xFF;
            }
            break;
    }

    return key;
}

// Static method to run from main
void StressTest::run_benchmark(const StressConfig& config) {
    try {
        auto storage = StorageFactory::create(config);
        StressTest test(std::move(storage), config);
        test.run();
    } catch (const std::exception& e) {
        std::cerr << "Benchmark failed: " << e.what() << std::endl;
        std::exit(1);
    }
}

} // namespace benchmark
#pragma once

#include <memory>
#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include <thread>
#include <string>
#include "storage/storage.h"
#include "benchmark/config.h"
#include "benchmark/metrics.h"

namespace benchmark {

// Main stress testing orchestrator
class StressTest {
public:
    StressTest(std::unique_ptr<Storage> storage, const StressConfig& config);

    // Run the complete benchmark
    void run();

    // Static method to run from main
    static void run_benchmark(const StressConfig& config);

private:
    std::unique_ptr<Storage> storage_;
    StressConfig config_;
    Metrics metrics_;
    std::vector<std::string> written_keys_;

    // Test phases
    void run_write_phase();
    void run_mixed_phase();

    // Thread functions
    void run_mixed_operations(size_t thread_id, size_t num_ops);
    void run_background_writes(size_t thread_id, std::atomic<bool>& stop);

    // Key generation
    std::string generate_key(size_t thread_id, size_t sequence, size_t key_len);
};

} // namespace benchmark
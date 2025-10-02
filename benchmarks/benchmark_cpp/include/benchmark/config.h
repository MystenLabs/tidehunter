#pragma once

#include <string>
#include <cstdint>

namespace benchmark {

// Configuration structure matching the Rust StressClientParameters
struct StressConfig {
    // Threading configuration
    size_t write_threads = 1;
    size_t mixed_threads = 1;

    // Operation counts
    size_t writes = 1000000;
    size_t operations = 1000000;

    // Data sizes
    size_t write_size = 1024;  // Value size in bytes
    size_t key_len = 32;       // Key length in bytes

    // Mixed phase configuration
    uint8_t read_percentage = 100;  // Percentage of reads vs writes
    size_t background_writes = 0;   // Background writes per second

    // Distribution configuration
    double zipf_exponent = 0.0;     // Zipf distribution exponent (0 = uniform)

    // Storage backend
    enum class Backend {
        ROCKSDB,
        BLOBDB,      // RocksDB with BlobDB enabled
        LMDB,
        FASTER,
        DIFFKV,
        TITAN,
        TERARKDB,
        PEBBLESDB
    };
    Backend backend = Backend::ROCKSDB;

    // Read operation mode
    enum class ReadMode {
        GET,         // Point lookup
        LT,          // Get values less than key (range query)
        EXISTS       // Check key existence
    };
    ReadMode read_mode = ReadMode::GET;
    size_t lt_iterations = 1;  // For ReadMode::LT

    // Key distribution pattern
    enum class KeyLayout {
        UNIFORM,
        SEQUENCE_CHOICE,
        CHOICE_SEQUENCE,
        ZIPF
    };
    KeyLayout key_layout = KeyLayout::UNIFORM;

    // Paths and options
    std::string path;        // Storage path (empty = temp directory)
    std::string reuse;       // Reuse existing database
    bool no_snapshot = false;
    bool preserve = false;    // Preserve database after benchmark
    bool report = false;      // Print detailed report
    std::string tldr;        // TLDR report tag

    // Parse from command line arguments
    static StressConfig parse(int argc, char* argv[]);

    // Load from YAML file
    static StressConfig load_yaml(const std::string& path);

    // Override with command line arguments
    void override_from_args(int argc, char* argv[]);

    // Get backend name as string
    std::string backend_name() const;

    // Get read mode as string
    std::string read_mode_name() const;
};

} // namespace benchmark
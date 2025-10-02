#include <iostream>
#include <iomanip>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <cstdlib>
#include <unistd.h>  // for gethostname

#include "benchmark/stress.h"
#include "benchmark/config.h"
#include "storage/storage.h"
#include "storage/storage_factory.h"

#ifdef HAS_BOOST
#include <boost/program_options.hpp>
namespace po = boost::program_options;
#endif

namespace fs = std::filesystem;

// Print message with timestamp matching Rust format
void report(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    std::cout << "[" << ms << "] " << message << std::endl;
}

// Get hostname
std::string get_hostname() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        return std::string(hostname);
    }
    return "<unknown>";
}

// Create temporary directory
fs::path create_temp_dir(const std::string& base_path = "") {
    fs::path temp_path;
    if (!base_path.empty()) {
        temp_path = fs::path(base_path) / "stress.XXXXXX";
    } else {
        temp_path = fs::temp_directory_path() / "stress.XXXXXX";
    }

    std::string temp_str = temp_path.string();
    char* temp_dir = mkdtemp(&temp_str[0]);
    if (!temp_dir) {
        throw std::runtime_error("Failed to create temporary directory");
    }
    return fs::path(temp_dir);
}

int main(int argc, char* argv[]) {
    try {
        report("BENCHMARK_START");

        // Parse configuration from command line and/or config file
        benchmark::StressConfig config;

#ifdef HAS_BOOST
        po::options_description desc("Benchmark options");
        desc.add_options()
            ("help,h", "Show help message")
            ("backend,b", po::value<std::string>()->default_value("rocksdb"),
             "Backend: rocksdb, blobdb, lmdb, faster, diffkv, titan")
            ("write-threads", po::value<size_t>()->default_value(1),
             "Number of write threads")
            ("mixed-threads", po::value<size_t>()->default_value(1),
             "Number of mixed read/write threads")
            ("writes,w", po::value<size_t>()->default_value(1000000),
             "Number of writes per thread")
            ("operations", po::value<size_t>()->default_value(1000000),
             "Operations in mixed phase")
            ("write-size,v", po::value<size_t>()->default_value(1024),
             "Value size in bytes")
            ("key-len,k", po::value<size_t>()->default_value(32),
             "Key length in bytes")
            ("read-percentage", po::value<uint8_t>()->default_value(100),
             "Percentage of reads in mixed phase")
            ("background-writes,u", po::value<size_t>()->default_value(0),
             "Background writes per second")
            ("key-layout", po::value<std::string>()->default_value("u"),
             "Key layout: u (uniform), sc (sequence-choice), cs (choice-sequence)")
            ("read-mode", po::value<std::string>()->default_value("get"),
             "Read mode: get, lt, exists")
            ("zipf-exponent", po::value<double>()->default_value(0.0),
             "Zipf distribution exponent (0 = uniform)")
            ("path,p", po::value<std::string>(),
             "Path for storage (default: temp directory)")
            ("reuse", po::value<std::string>(),
             "Reuse existing database at path")
            ("no-snapshot,n", "Disable periodic snapshots")
            ("preserve", "Preserve database after benchmark")
            ("report", "Print detailed report")
            ("tldr", po::value<std::string>(),
             "TLDR report tag")
            ("config", po::value<std::string>(),
             "Path to YAML config file");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return 0;
        }

        // Parse backend
        std::string backend_str = vm["backend"].as<std::string>();
        if (backend_str == "rocksdb") {
            config.backend = benchmark::StressConfig::Backend::ROCKSDB;
        } else if (backend_str == "blobdb") {
            config.backend = benchmark::StressConfig::Backend::BLOBDB;
        } else if (backend_str == "lmdb") {
            config.backend = benchmark::StressConfig::Backend::LMDB;
        } else {
            throw std::runtime_error("Unknown backend: " + backend_str);
        }

        // Parse other options
        config.write_threads = vm["write-threads"].as<size_t>();
        config.mixed_threads = vm["mixed-threads"].as<size_t>();
        config.writes = vm["writes"].as<size_t>();
        config.operations = vm["operations"].as<size_t>();
        config.write_size = vm["write-size"].as<size_t>();
        config.key_len = vm["key-len"].as<size_t>();
        config.read_percentage = vm["read-percentage"].as<uint8_t>();
        config.background_writes = vm["background-writes"].as<size_t>();
        config.zipf_exponent = vm["zipf-exponent"].as<double>();
        config.no_snapshot = vm.count("no-snapshot") > 0;
        config.preserve = vm.count("preserve") > 0;
        config.report = vm.count("report") > 0;

        if (vm.count("path")) {
            config.path = vm["path"].as<std::string>();
        }
        if (vm.count("reuse")) {
            config.reuse = vm["reuse"].as<std::string>();
        }
        if (vm.count("tldr")) {
            config.tldr = vm["tldr"].as<std::string>();
        }

        // Parse key layout
        std::string layout_str = vm["key-layout"].as<std::string>();
        if (layout_str == "u") {
            config.key_layout = benchmark::StressConfig::KeyLayout::UNIFORM;
        } else if (layout_str == "sc") {
            config.key_layout = benchmark::StressConfig::KeyLayout::SEQUENCE_CHOICE;
        } else if (layout_str == "cs") {
            config.key_layout = benchmark::StressConfig::KeyLayout::CHOICE_SEQUENCE;
        }

        // Parse read mode
        std::string mode_str = vm["read-mode"].as<std::string>();
        if (mode_str == "get") {
            config.read_mode = benchmark::StressConfig::ReadMode::GET;
        } else if (mode_str.substr(0, 2) == "lt") {
            config.read_mode = benchmark::StressConfig::ReadMode::LT;
            if (mode_str.length() > 3) {
                config.lt_iterations = std::stoi(mode_str.substr(3));
            }
        } else if (mode_str == "exists") {
            config.read_mode = benchmark::StressConfig::ReadMode::EXISTS;
        }

#else
        // Simple argument parsing without Boost
        config = benchmark::StressConfig::parse(argc, argv);
#endif

        // Print configuration
        std::cout << "DB parameters: {" << std::endl;
        std::cout << "  backend: " << config.backend_name() << std::endl;
        std::cout << "}" << std::endl;

        std::cout << "Stress client parameters: {" << std::endl;
        std::cout << "  write_threads: " << config.write_threads << std::endl;
        std::cout << "  mixed_threads: " << config.mixed_threads << std::endl;
        std::cout << "  writes: " << config.writes << std::endl;
        std::cout << "  operations: " << config.operations << std::endl;
        std::cout << "  write_size: " << config.write_size << std::endl;
        std::cout << "  key_len: " << config.key_len << std::endl;
        std::cout << "  read_percentage: " << (int)config.read_percentage << std::endl;
        std::cout << "  background_writes: " << config.background_writes << std::endl;
        std::cout << "  key_layout: " <<
            (config.key_layout == benchmark::StressConfig::KeyLayout::UNIFORM ? "Uniform" :
             config.key_layout == benchmark::StressConfig::KeyLayout::SEQUENCE_CHOICE ? "SequenceChoice" :
             "ChoiceSequence") << std::endl;
        std::cout << "  read_mode: " << config.read_mode_name() << std::endl;
        if (!config.tldr.empty()) {
            std::cout << "  tldr: \"" << config.tldr << "\"" << std::endl;
        }
        std::cout << "  preserve: " << (config.preserve ? "true" : "false") << std::endl;
        std::cout << "}" << std::endl;

        // Determine storage path
        if (!config.reuse.empty()) {
            config.path = config.reuse;
            report("Reusing database at: " + config.path);
        } else if (config.path.empty()) {
            config.path = create_temp_dir().string();
        } else {
            config.path = create_temp_dir(config.path).string();
        }

        // Print system information
        report("Hostname: " + get_hostname());
        report("Path to storage: " + config.path);
        report(std::string("Using ") +
            (config.key_layout == benchmark::StressConfig::KeyLayout::UNIFORM ? "Uniform" :
             config.key_layout == benchmark::StressConfig::KeyLayout::SEQUENCE_CHOICE ? "SequenceChoice" :
             "ChoiceSequence") + " key layout");
        report("Using " + config.read_mode_name() + " read mode");

        // Run the benchmark using the static method
        benchmark::StressTest::run_benchmark(config);

        // Clean up if not preserving
        if (!config.preserve && config.reuse.empty()) {
            try {
                fs::remove_all(config.path);
            } catch (const std::exception& e) {
                std::cerr << "Failed to clean up: " << e.what() << std::endl;
            }
        }

        report("BENCHMARK_END");
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
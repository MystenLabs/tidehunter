#include "benchmark/config.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>

namespace benchmark {

StressConfig StressConfig::parse(int argc, char* argv[]) {
    StressConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-b" || arg == "--backend") {
            if (++i < argc) {
                std::string backend = argv[i];
                if (backend == "rocksdb") {
                    config.backend = Backend::ROCKSDB;
                } else if (backend == "blobdb") {
                    config.backend = Backend::BLOBDB;
                } else if (backend == "lmdb") {
                    config.backend = Backend::LMDB;
                }
            }
        } else if (arg == "-w" || arg == "--writes") {
            if (++i < argc) {
                config.writes = std::stoull(argv[i]);
            }
        } else if (arg == "--operations") {
            if (++i < argc) {
                config.operations = std::stoull(argv[i]);
            }
        } else if (arg == "-v" || arg == "--write-size") {
            if (++i < argc) {
                config.write_size = std::stoull(argv[i]);
            }
        } else if (arg == "-k" || arg == "--key-len") {
            if (++i < argc) {
                config.key_len = std::stoull(argv[i]);
            }
        } else if (arg == "-p" || arg == "--path") {
            if (++i < argc) {
                config.path = argv[i];
            }
        } else if (arg == "--write-threads") {
            if (++i < argc) {
                config.write_threads = std::stoull(argv[i]);
            }
        } else if (arg == "--mixed-threads") {
            if (++i < argc) {
                config.mixed_threads = std::stoull(argv[i]);
            }
        } else if (arg == "--read-percentage") {
            if (++i < argc) {
                config.read_percentage = std::stoi(argv[i]);
            }
        } else if (arg == "--tldr") {
            if (++i < argc) {
                config.tldr = argv[i];
            }
        } else if (arg == "--preserve") {
            config.preserve = true;
        } else if (arg == "--no-snapshot" || arg == "-n") {
            config.no_snapshot = true;
        }
    }

    return config;
}

StressConfig StressConfig::load_yaml(const std::string& path) {
    // TODO: Implement YAML loading if needed
    return StressConfig();
}

void StressConfig::override_from_args(int argc, char* argv[]) {
    *this = parse(argc, argv);
}

std::string StressConfig::backend_name() const {
    switch (backend) {
        case Backend::ROCKSDB: return "rocksdb";
        case Backend::BLOBDB: return "blobdb";
        case Backend::LMDB: return "lmdb";
        case Backend::FASTER: return "faster";
        case Backend::DIFFKV: return "diffkv";
        case Backend::TITAN: return "titan";
        case Backend::TERARKDB: return "terarkdb";
        case Backend::PEBBLESDB: return "pebblesdb";
        default: return "unknown";
    }
}

std::string StressConfig::read_mode_name() const {
    switch (read_mode) {
        case ReadMode::GET: return "Get";
        case ReadMode::LT: return "Lt(" + std::to_string(lt_iterations) + ")";
        case ReadMode::EXISTS: return "Exists";
        default: return "Unknown";
    }
}

} // namespace benchmark
#include "storage/storage_factory.h"
#include <stdexcept>
#include <filesystem>
#include <chrono>

#ifdef HAS_ROCKSDB
#include "storage/rocksdb_storage.h"
#endif

#ifdef HAS_LMDB
#include "storage/lmdb_storage.h"
#endif

#ifdef HAS_FASTER
// #include "storage/faster_storage.h"
#endif

#ifdef HAS_DIFFKV
// #include "storage/diffkv_storage.h"
#endif

#ifdef HAS_TITAN
// #include "storage/titan_storage.h"
#endif

namespace benchmark {

std::unique_ptr<Storage> StorageFactory::create(const StressConfig& config) {
    std::string db_path = config.path;
    if (db_path.empty()) {
        // Create temporary directory with backend name
        db_path = "/tmp/benchmark_" + config.backend_name() + "_" +
                  std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }

    // Clean up existing directory if not preserving
    if (!config.preserve && std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
    }

    switch (config.backend) {
#ifdef HAS_ROCKSDB
        case StressConfig::Backend::ROCKSDB: {
            auto storage = std::make_unique<RocksDBStorage>(db_path);
            storage->optimize_for_write_throughput();
            return storage;
        }
        case StressConfig::Backend::BLOBDB: {
            auto storage = std::make_unique<RocksDBStorage>(db_path);
            storage->enable_blob_db();
            storage->optimize_for_write_throughput();
            return storage;
        }
#endif

#ifdef HAS_LMDB
        case StressConfig::Backend::LMDB: {
            auto storage = std::make_unique<LMDBStorage>(db_path);
            // Configure LMDB for benchmark workload
            storage->set_map_size(1ULL << 40); // 1TB
            return storage;
        }
#endif

#ifdef HAS_FASTER
        case StressConfig::Backend::FASTER: {
            // return std::make_unique<FASTERStorage>(db_path);
            throw std::runtime_error("FASTER backend not yet implemented");
        }
#endif

#ifdef HAS_DIFFKV
        case StressConfig::Backend::DIFFKV: {
            // return std::make_unique<DiffKVStorage>(db_path);
            throw std::runtime_error("DiffKV backend not yet implemented");
        }
#endif

#ifdef HAS_TITAN
        case StressConfig::Backend::TITAN: {
            // return std::make_unique<TitanStorage>(db_path);
            throw std::runtime_error("Titan backend not yet implemented");
        }
        case StressConfig::Backend::TERARKDB: {
            throw std::runtime_error("TerarkDB backend not yet implemented");
        }
        case StressConfig::Backend::PEBBLESDB: {
            throw std::runtime_error("PebblesDB backend not yet implemented");
        }
#endif

        default:
            throw std::runtime_error("Backend not compiled in or not supported: " +
                                   config.backend_name());
    }
}

} // namespace benchmark
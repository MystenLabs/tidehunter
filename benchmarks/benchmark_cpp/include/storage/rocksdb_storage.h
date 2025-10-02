#pragma once

#ifdef HAS_ROCKSDB

#include "storage/storage.h"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <memory>

namespace benchmark {

class RocksDBStorage : public Storage {
public:
    // Constructor with path and optional BlobDB configuration
    RocksDBStorage(const std::string& path, bool use_blob_db = false);
    ~RocksDBStorage() override;

    // Storage interface implementation
    void insert(const std::string& key, const std::string& value) override;
    std::optional<std::string> get(const std::string& key) override;
    std::vector<std::string> get_lt(const std::string& key, size_t iterations) override;
    bool exists(const std::string& key) override;
    const char* name() const override;

    // Configuration methods (made public for factory)
    void optimize_for_write_throughput();
    void enable_blob_db();

private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options_;
    bool use_blob_db_;

    // Private configuration methods
    void configure_options();
    void update_options();

    static rocksdb::BlockBasedTableOptions get_block_options(size_t block_cache_size_mb, size_t block_size_bytes);
};

} // namespace benchmark

#endif // HAS_ROCKSDB
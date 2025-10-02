#ifdef HAS_ROCKSDB

#include "storage/rocksdb_storage.h"
#include <rocksdb/iterator.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <iostream>
#include <filesystem>

namespace benchmark {

RocksDBStorage::RocksDBStorage(const std::string& path, bool use_blob_db)
    : use_blob_db_(use_blob_db) {

    // Create directory if it doesn't exist
    std::filesystem::create_directories(path);

    // Configure options
    configure_options();

    // Open database
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options_, path, &db);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    db_.reset(db);
}

RocksDBStorage::~RocksDBStorage() {
    if (db_) {
        db_->FlushWAL(true);
        db_.reset();
    }
}

void RocksDBStorage::configure_options() {
    options_.create_if_missing = true;
    options_.create_missing_column_families = true;

    update_options();
    optimize_for_write_throughput();

    if (use_blob_db_) {
        enable_blob_db();
    }
}

void RocksDBStorage::optimize_for_write_throughput() {
    // Matching Rust implementation
    const size_t DEFAULT_MAX_WRITE_BUFFER_SIZE_MB = 256;
    const int DEFAULT_MAX_WRITE_BUFFER_NUMBER = 6;
    const int DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER = 4;
    const size_t DEFAULT_TARGET_FILE_SIZE_BASE_MB = 128;

    // Increase write buffer size to 256MiB
    size_t write_buffer_size = DEFAULT_MAX_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options_.write_buffer_size = write_buffer_size;

    // Increase write buffers to keep to 6 before slowing down writes
    options_.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;

    // Keep 1 write buffer so recent writes can be read from memory
    options_.max_write_buffer_size_to_maintain = write_buffer_size;

    // Increase compaction trigger for level 0 to 4
    options_.level0_file_num_compaction_trigger = DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER;
    options_.level0_slowdown_writes_trigger = DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER * 12;
    options_.level0_stop_writes_trigger = DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER * 16;

    // Increase sst file size to 128MiB
    options_.target_file_size_base = DEFAULT_TARGET_FILE_SIZE_BASE_MB * 1024 * 1024;

    // Increase level 1 target size
    options_.max_bytes_for_level_base = write_buffer_size * DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER;

    // Set max open files
    options_.max_open_files = 5000;
}

void RocksDBStorage::update_options() {
    const size_t DEFAULT_DB_WRITE_BUFFER_SIZE = 1024;
    const size_t DEFAULT_DB_WAL_SIZE = 1024;

    options_.table_cache_numshardbits = 10;

    // LSM compression settings
    options_.compression = rocksdb::kLZ4Compression;
    options_.bottommost_compression = rocksdb::kZSTD;

    // Write buffer and WAL limits
    options_.db_write_buffer_size = DEFAULT_DB_WRITE_BUFFER_SIZE * 1024 * 1024;
    options_.max_total_wal_size = DEFAULT_DB_WAL_SIZE * 1024 * 1024;

    // Parallelism
    options_.max_background_compactions = 4;
    options_.max_background_flushes = 2;
    options_.env->SetBackgroundThreads(4, rocksdb::Env::LOW);
    options_.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);

    // Enable pipelined writes
    options_.enable_pipelined_write = true;

    // Set block-based table options
    rocksdb::BlockBasedTableOptions table_options = get_block_options(128, 16 * 1024);
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // Set memtable bloom filter
    options_.memtable_prefix_bloom_size_ratio = 0.02;
}

void RocksDBStorage::enable_blob_db() {
    // Enable integrated BlobDB
    options_.enable_blob_files = true;

    // Values smaller than this remain in LSM
    options_.min_blob_size = 256;

    // Size of blob files before rolling
    options_.blob_file_size = 128 * 1024 * 1024;

    // Compression for blobs
    options_.blob_compression_type = rocksdb::kZSTD;

    // Garbage collection configuration
    options_.blob_garbage_collection_age_cutoff = 0.25;
    options_.blob_garbage_collection_force_threshold = 0.75;

    // Compaction readahead for blob files (0 disables)
    options_.blob_compaction_readahead_size = 0;
}

rocksdb::BlockBasedTableOptions RocksDBStorage::get_block_options(
    size_t block_cache_size_mb, size_t block_size_bytes) {

    rocksdb::BlockBasedTableOptions block_options;

    // Set block size
    block_options.block_size = block_size_bytes;

    // Configure block cache
    block_options.block_cache = rocksdb::NewLRUCache(block_cache_size_mb << 20);

    // Set bloom filter
    block_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0, false));

    // Pin L0 index and filter blocks
    block_options.pin_l0_filter_and_index_blocks_in_cache = true;

    return block_options;
}

void RocksDBStorage::insert(const std::string& key, const std::string& value) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("Insert failed: " + status.ToString());
    }
}

std::optional<std::string> RocksDBStorage::get(const std::string& key) {
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    if (status.ok()) {
        return value;
    } else if (status.IsNotFound()) {
        return std::nullopt;
    } else {
        throw std::runtime_error("Get failed: " + status.ToString());
    }
}

std::vector<std::string> RocksDBStorage::get_lt(const std::string& key, size_t iterations) {
    std::vector<std::string> result;
    result.reserve(iterations);

    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options));

    // Seek to the key and go backwards
    iter->Seek(key);
    if (iter->Valid() && iter->key().ToString() >= key) {
        iter->Prev();
    }

    // Collect up to 'iterations' values
    for (size_t i = 0; i < iterations && iter->Valid(); ++i) {
        result.push_back(iter->value().ToString());
        iter->Prev();
    }

    return result;
}

bool RocksDBStorage::exists(const std::string& key) {
    // RocksDB doesn't have a native exists method, so we use get
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    return status.ok();
}

const char* RocksDBStorage::name() const {
    return use_blob_db_ ? "blobdb" : "rocksdb";
}

} // namespace benchmark

#endif // HAS_ROCKSDB
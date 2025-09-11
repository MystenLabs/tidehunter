use crate::storage::Storage;
use minibytes::Bytes;
use rocksdb::{BlockBasedOptions, Cache, Direction, IteratorMode, Options, DB};
use std::path::Path;
use std::sync::Arc;

pub struct RocksStorage {
    db: DB,
    mode: RocksMode,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum RocksMode {
    Plain,
    BlobDb,
}

impl RocksStorage {
    pub fn open(path: &Path, use_blob_store: bool, metrics_enabled: bool) -> Arc<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        Self::update_opts(&mut opts);
        Self::optimize_for_write_throughput(&mut opts);
        if use_blob_store {
            // Enable integrated BlobDB with sensible defaults
            Self::enable_blobdb(&mut opts);
        }
        // Enable or disable RocksDB statistics based on flag
        if metrics_enabled {
            opts.enable_statistics();
        }

        std::fs::create_dir_all(path).unwrap();
        let db = DB::open(&opts, path).unwrap();
        let mode = if use_blob_store {
            RocksMode::BlobDb
        } else {
            RocksMode::Plain
        };
        Arc::new(Self { db, mode })
    }

    pub fn optimize_for_write_throughput(opt: &mut Options) {
        const DEFAULT_MAX_WRITE_BUFFER_SIZE_MB: usize = 256;
        const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 6;
        const DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER: usize = 4;
        const DEFAULT_TARGET_FILE_SIZE_BASE_MB: usize = 128;
        // Increase write buffer size to 256MiB.
        let write_buffer_size = DEFAULT_MAX_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
        opt.set_write_buffer_size(write_buffer_size);
        // Increase write buffers to keep to 6 before slowing down writes.
        let max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;
        opt.set_max_write_buffer_number(max_write_buffer_number.try_into().unwrap());
        // Keep 1 write buffer so recent writes can be read from memory.
        opt.set_max_write_buffer_size_to_maintain((write_buffer_size).try_into().unwrap());

        // Increase compaction trigger for level 0 to 6.
        let max_level_zero_file_num = DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER;
        opt.set_level_zero_file_num_compaction_trigger(max_level_zero_file_num.try_into().unwrap());
        opt.set_level_zero_slowdown_writes_trigger(
            (max_level_zero_file_num * 12).try_into().unwrap(),
        );
        opt.set_level_zero_stop_writes_trigger((max_level_zero_file_num * 16).try_into().unwrap());

        // Increase sst file size to 128MiB.
        opt.set_target_file_size_base(DEFAULT_TARGET_FILE_SIZE_BASE_MB as u64 * 1024 * 1024);

        // Increase level 1 target size to 256MiB * 6 ~ 1.5GiB.
        opt.set_max_bytes_for_level_base((write_buffer_size * max_level_zero_file_num) as u64);

        // One common issue is that the default ulimit is too low,
        // leading to I/O errors such as "Too many open files". Raising fdlimit to bypass it.
        if let Some(limit) = fdlimit::raise_fd_limit() {
            println!("Raised fdlimit to {}", limit);
            // on windows raise_fd_limit return None
            opt.set_max_open_files((limit / 8) as i32);
        }
    }

    fn update_opts(opt: &mut Options) {
        const DEFAULT_DB_WRITE_BUFFER_SIZE: usize = 1024;
        const DEFAULT_DB_WAL_SIZE: usize = 1024;
        opt.set_table_cache_num_shard_bits(10);

        // LSM compression settings
        opt.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opt.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opt.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

        // Sui uses multiple RocksDB in a node, so total sizes of write buffers and WAL can be higher
        // than the limits below.
        //
        // RocksDB also exposes the option to configure total write buffer size across multiple instances
        // via `write_buffer_manager`. But the write buffer flush policy (flushing the buffer receiving
        // the next write) may not work well. So sticking to per-db write buffer size limit for now.
        //
        // The environment variables are only meant to be emergency overrides. They may go away in future.
        // It is preferable to update the default value, or override the option in code.
        opt.set_db_write_buffer_size(DEFAULT_DB_WRITE_BUFFER_SIZE * 1024 * 1024);
        opt.set_max_total_wal_size(DEFAULT_DB_WAL_SIZE as u64 * 1024 * 1024);

        // Num threads for compactions and memtable flushes.
        opt.increase_parallelism(8);

        opt.set_enable_pipelined_write(true);

        // Increase block size to 16KiB.
        // https://github.com/EighteenZi/rocksdb_wiki/blob/master/Memory-usage-in-RocksDB.md#indexes-and-filter-blocks
        opt.set_block_based_table_factory(&get_block_options(128, 16 << 10));

        // Set memtable bloomfilter.
        opt.set_memtable_prefix_bloom_ratio(0.02);
    }

    fn enable_blobdb(opt: &mut Options) {
        // Integrated BlobDB switches
        opt.set_enable_blob_files(true);
        // Values smaller than this remain in LSM; keep small threshold as previously used
        opt.set_min_blob_size(256);
        // Size of blob files before rolling
        opt.set_blob_file_size(128 * 1024 * 1024);
        // Compression for blobs; ZSTD for better ratio
        opt.set_blob_compression_type(rocksdb::DBCompressionType::Zstd);
        // Readahead for compaction over blob files (0 disables)
        opt.set_blob_compaction_readahead_size(0);
    }
}

impl Storage for Arc<RocksStorage> {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.db.put(&k, &v).unwrap()
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.db.get(k).unwrap().map(Into::into)
    }

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes> {
        let mut iterator = self.db.iterator(IteratorMode::From(k, Direction::Reverse));
        let mut result = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let next = iterator.next();
            if let Some(next) = next {
                result.push(next.expect("Db error").1.into());
            } else {
                break;
            }
        }
        result
    }

    fn exists(&self, k: &[u8]) -> bool {
        // RocksDB doesn't have a native exists method, so we use get and check if it's Some
        self.db.get(k).unwrap().is_some()
    }

    fn name(&self) -> &'static str {
        match self.mode {
            RocksMode::Plain => "rocksdb",
            RocksMode::BlobDb => "blobdb",
        }
    }
}

impl Drop for RocksStorage {
    fn drop(&mut self) {
        self.db.cancel_all_background_work(true);
    }
}

fn get_block_options(block_cache_size_mb: usize, block_size_bytes: usize) -> BlockBasedOptions {
    // Set options mostly similar to those used in optimize_for_point_lookup(),
    // except non-default binary and hash index, to hopefully reduce lookup latencies
    // without causing any regression for scanning, with slightly more memory usages.
    // https://github.com/facebook/rocksdb/blob/11cb6af6e5009c51794641905ca40ce5beec7fee/options/options.cc#L611-L621
    let mut block_options = BlockBasedOptions::default();
    // Overrides block size.
    block_options.set_block_size(block_size_bytes);
    // Configure a block cache.
    block_options.set_block_cache(&Cache::new_lru_cache(block_cache_size_mb << 20));
    // Set a bloomfilter with 1% false positive rate.
    block_options.set_bloom_filter(10.0, false);
    // From https://github.com/EighteenZi/rocksdb_wiki/blob/master/Block-Cache.md#caching-index-and-filter-blocks
    block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_options
}

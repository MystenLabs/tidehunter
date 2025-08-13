use crate::storage::Storage;
use minibytes::Bytes;
use rocksdb::{BlockBasedOptions, Cache, Direction, IteratorMode, Options, DB};
use std::path::Path;
use std::sync::Arc;

pub struct BlobDbStorage {
    db: DB,
}

impl BlobDbStorage {
    pub fn open(path: &Path) -> Arc<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // General DB optimizations similar to RocksStorage
        Self::update_general_opts(&mut opts);
        Self::optimize_for_write_throughput(&mut opts);

        // Enable integrated BlobDB per RocksDB wiki guidance
        // https://github.com/facebook/rocksdb/wiki/BlobDB
        opts.set_enable_blob_files(true);
        // Values smaller than this remain in LSM; tune for large-value workloads
        // Using 4KiB default threshold
        opts.set_min_blob_size(4 * 1024);
        // Size of blob files before rolling
        opts.set_blob_file_size(128 * 1024 * 1024);
        // Compression for blobs; ZSTD for better ratio
        opts.set_blob_compression_type(rocksdb::DBCompressionType::Zstd);
        // Readahead for compaction over blob files (0 disables)
        opts.set_blob_compaction_readahead_size(0);

        std::fs::create_dir_all(path).unwrap();
        let db = DB::open(&opts, path).unwrap();
        Arc::new(Self { db })
    }

    fn optimize_for_write_throughput(opt: &mut Options) {
        const DEFAULT_MAX_WRITE_BUFFER_SIZE_MB: usize = 256;
        const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 6;
        const DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER: usize = 4;
        const DEFAULT_TARGET_FILE_SIZE_BASE_MB: usize = 128;
        let write_buffer_size = DEFAULT_MAX_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
        opt.set_write_buffer_size(write_buffer_size);
        opt.set_max_write_buffer_number(DEFAULT_MAX_WRITE_BUFFER_NUMBER.try_into().unwrap());
        opt.set_max_write_buffer_size_to_maintain((write_buffer_size).try_into().unwrap());
        opt.set_level_zero_file_num_compaction_trigger(
            DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER.try_into().unwrap(),
        );
        opt.set_level_zero_slowdown_writes_trigger(
            (DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER * 12)
                .try_into()
                .unwrap(),
        );
        opt.set_level_zero_stop_writes_trigger(
            (DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER * 16)
                .try_into()
                .unwrap(),
        );
        opt.set_target_file_size_base(DEFAULT_TARGET_FILE_SIZE_BASE_MB as u64 * 1024 * 1024);
        opt.set_max_bytes_for_level_base(
            (write_buffer_size * DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER) as u64,
        );
        if let Some(limit) = fdlimit::raise_fd_limit() {
            opt.set_max_open_files((limit / 8) as i32);
        }
    }

    fn update_general_opts(opt: &mut Options) {
        const DEFAULT_DB_WRITE_BUFFER_SIZE: usize = 1024;
        const DEFAULT_DB_WAL_SIZE: usize = 1024;
        opt.set_table_cache_num_shard_bits(10);
        opt.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opt.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opt.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);
        opt.set_db_write_buffer_size(DEFAULT_DB_WRITE_BUFFER_SIZE * 1024 * 1024);
        opt.set_max_total_wal_size((DEFAULT_DB_WAL_SIZE as u64) * 1024 * 1024);
        opt.increase_parallelism(8);
        opt.set_enable_pipelined_write(true);
        opt.set_block_based_table_factory(&get_block_options(128, 16 << 10));
        opt.set_memtable_prefix_bloom_ratio(0.02);
    }
}

impl Storage for Arc<BlobDbStorage> {
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
        self.db.get(k).unwrap().is_some()
    }

    fn name(&self) -> &'static str {
        "blobdb"
    }
}

impl Drop for BlobDbStorage {
    fn drop(&mut self) {
        self.db.cancel_all_background_work(true);
    }
}

fn get_block_options(block_cache_size_mb: usize, block_size_bytes: usize) -> BlockBasedOptions {
    let mut block_options = BlockBasedOptions::default();
    block_options.set_block_size(block_size_bytes);
    block_options.set_block_cache(&Cache::new_lru_cache(block_cache_size_mb << 20));
    block_options.set_bloom_filter(10.0, false);
    block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_options
}

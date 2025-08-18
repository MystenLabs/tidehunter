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
    pub fn open(path: &Path, use_blob_store: bool) -> Arc<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        Self::update_opts(&mut opts);
        Self::optimize_for_write_throughput(&mut opts);
        if use_blob_store {
            // Enable integrated BlobDB with sensible defaults
            Self::enable_blobdb(&mut opts);
        }

        opts.enable_statistics();

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

    fn cache_hit_report(&self) -> String {
        // Pull the aggregated RocksDB stats string. This requires RocksDB statistics to be enabled.
        // If stats are not enabled, this will likely be None or contain no tickers and we return an empty string.
        let Ok(Some(stats)) = self.db.property_value("rocksdb.stats") else {
            return String::new();
        };

        // Helper to extract a u64 ticker value from the stats string by name.
        // The stats string usually includes lines like:
        //   MEMTABLE_HIT: 123
        //   BLOCK_CACHE_DATA_HIT: 456
        fn parse_ticker(stats: &str, name: &str) -> u64 {
            let mut total: u64 = 0;
            for line in stats.lines() {
                // Match lines that contain the ticker name as a whole word to reduce false positives.
                if line.contains(name) {
                    // Extract the last integer on the line
                    let mut last_num: Option<u64> = None;
                    for token in line.split(|c: char| !c.is_ascii_alphanumeric()) {
                        if token.is_empty() {
                            continue;
                        }
                        if let Ok(v) = token.parse::<u64>() {
                            last_num = Some(v);
                        }
                    }
                    if let Some(v) = last_num {
                        total = v;
                    }
                }
            }
            total
        }

        // Core request-path metrics
        let mem_hit = parse_ticker(&stats, "MEMTABLE_HIT");
        let mem_miss = parse_ticker(&stats, "MEMTABLE_MISS");
        let total_gets = mem_hit.saturating_add(mem_miss);

        // Level where GET was satisfied (only for found keys in SST)
        let l0_hits = parse_ticker(&stats, "GET_HIT_L0");
        let l1_hits = parse_ticker(&stats, "GET_HIT_L1");
        let l2_up_hits = parse_ticker(&stats, "GET_HIT_L2_AND_UP");
        let total_sst_level_hits = l0_hits.saturating_add(l1_hits).saturating_add(l2_up_hits);

        // Block cache (overall and breakdowns). Note these are block-level counters, not request-level.
        let bc_hit = parse_ticker(&stats, "BLOCK_CACHE_HIT");
        let bc_miss = parse_ticker(&stats, "BLOCK_CACHE_MISS");
        let bc_data_hit = parse_ticker(&stats, "BLOCK_CACHE_DATA_HIT");
        let bc_data_miss = parse_ticker(&stats, "BLOCK_CACHE_DATA_MISS");
        let bc_index_hit = parse_ticker(&stats, "BLOCK_CACHE_INDEX_HIT");
        let bc_index_miss = parse_ticker(&stats, "BLOCK_CACHE_INDEX_MISS");
        let bc_filter_hit = parse_ticker(&stats, "BLOCK_CACHE_FILTER_HIT");
        let bc_filter_miss = parse_ticker(&stats, "BLOCK_CACHE_FILTER_MISS");

        // Disk reads (block-level). Names differ across versions; try both common variants.
        let mut block_read_count = parse_ticker(&stats, "BLOCK_READ_COUNT");
        if block_read_count == 0 {
            block_read_count = parse_ticker(&stats, "BLOCKS_READ");
        }
        let mut block_read_bytes = parse_ticker(&stats, "BLOCK_READ_BYTES");
        if block_read_bytes == 0 {
            block_read_bytes = parse_ticker(&stats, "BYTES_READ");
        }

        // Bloom filter effectiveness
        let bloom_useful = parse_ticker(&stats, "BLOOM_FILTER_USEFUL");
        // Some builds expose BLOOM_FILTER_CHECKED; others expose FULL_* variants. Capture all we can.
        let bloom_checked = parse_ticker(&stats, "BLOOM_FILTER_CHECKED");
        let bloom_full_pos = parse_ticker(&stats, "BLOOM_FILTER_FULL_POSITIVE");
        let _bloom_full_true = parse_ticker(&stats, "BLOOM_FILTER_FULL_TRUE_POSITIVE");

        // Other caches if enabled
        let row_cache_hit = parse_ticker(&stats, "ROW_CACHE_HIT");
        let row_cache_miss = parse_ticker(&stats, "ROW_CACHE_MISS");
        let persistent_cache_hit = parse_ticker(&stats, "PERSISTENT_CACHE_HIT");
        let persistent_cache_miss = parse_ticker(&stats, "PERSISTENT_CACHE_MISS");
        let sim_bc_hit = parse_ticker(&stats, "SIM_BLOCK_CACHE_HIT");
        let sim_bc_miss = parse_ticker(&stats, "SIM_BLOCK_CACHE_MISS");

        // Ratios with safe divisions
        let ratio = |num: u64, den: u64| -> f64 {
            if den == 0 {
                0.0
            } else {
                (num as f64) / (den as f64)
            }
        };

        let mem_hit_ratio = ratio(mem_hit, total_gets);
        // Among requests that missed memtable, many are satisfied from SST. We can't perfectly map per-request
        // block cache effectiveness, but we report data-block hit ratio which is the most relevant for values.
        let data_bc_total = bc_data_hit.saturating_add(bc_data_miss);
        let data_bc_hit_ratio = ratio(bc_data_hit, data_bc_total);
        let overall_bc_total = bc_hit.saturating_add(bc_miss);
        let overall_bc_hit_ratio = ratio(bc_hit, overall_bc_total);

        let level_total = total_sst_level_hits;
        let l0_ratio = ratio(l0_hits, level_total);
        let l1_ratio = ratio(l1_hits, level_total);
        let l2up_ratio = ratio(l2_up_hits, level_total);

        let bloom_checked_total = if bloom_checked > 0 {
            bloom_checked
        } else {
            bloom_useful.saturating_add(bloom_full_pos)
        };
        let bloom_effectiveness = ratio(bloom_useful, bloom_checked_total);

        let row_cache_total = row_cache_hit.saturating_add(row_cache_miss);
        let row_cache_hit_ratio = ratio(row_cache_hit, row_cache_total);

        let persistent_cache_total = persistent_cache_hit.saturating_add(persistent_cache_miss);
        let persistent_cache_hit_ratio = ratio(persistent_cache_hit, persistent_cache_total);

        let sim_bc_total = sim_bc_hit.saturating_add(sim_bc_miss);
        let sim_bc_hit_ratio = ratio(sim_bc_hit, sim_bc_total);

        // Compose report
        let mut out = String::new();
        use std::fmt::Write as _;
        let _ = write!(
	            &mut out,
	            "RocksDB cache hit report\n\
	            - MemTable: hits={} misses={} total={} hit_ratio={:.4}\n\
	            - SST level hits (found keys only): L0={} ({:.4}) L1={} ({:.4}) L2+={} ({:.4}) total={}\n\
	            - Block cache (overall blocks): hits={} misses={} hit_ratio={:.4}\n\
	            - Block cache (data blocks): hits={} misses={} hit_ratio={:.4}\n\
	              Index blocks: hits={} misses={} | Filter blocks: hits={} misses={}\n\
	            - Disk reads (blocks): count={} bytes={}\n\
	            - Bloom filter: useful={} checked={} effectiveness={:.4}\n",
	            mem_hit,
	            mem_miss,
	            total_gets,
	            mem_hit_ratio,
	            l0_hits,
	            l0_ratio,
	            l1_hits,
	            l1_ratio,
	            l2_up_hits,
	            l2up_ratio,
	            level_total,
	            bc_hit,
	            bc_miss,
	            overall_bc_hit_ratio,
	            bc_data_hit,
	            bc_data_miss,
	            data_bc_hit_ratio,
	            bc_index_hit,
	            bc_index_miss,
	            bc_filter_hit,
	            bc_filter_miss,
	            block_read_count,
	            block_read_bytes,
	            bloom_useful,
	            bloom_checked_total,
	            bloom_effectiveness,
	        );

        if row_cache_total > 0 {
            let _ = write!(
                &mut out,
                "- Row cache: hits={} misses={} hit_ratio={:.4}\n",
                row_cache_hit, row_cache_miss, row_cache_hit_ratio
            );
        }
        if persistent_cache_total > 0 {
            let _ = write!(
                &mut out,
                "- Persistent cache: hits={} misses={} hit_ratio={:.4}\n",
                persistent_cache_hit, persistent_cache_miss, persistent_cache_hit_ratio
            );
        }
        if sim_bc_total > 0 {
            let _ = write!(
                &mut out,
                "- Simulated block cache: hits={} misses={} hit_ratio={:.4}\n",
                sim_bc_hit, sim_bc_miss, sim_bc_hit_ratio
            );
        }

        out
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

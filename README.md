# Tidehunter

A high-performance, embedded key-value storage engine written in Rust, designed for distributed systems with a focus on durability, performance, and flexibility.

> **Reproducing the paper's experiments:** see [REPRODUCING.md](REPRODUCING.md) for step-by-step instructions to reproduce every experiment in *"Tidehunter: Large-Value Storage With Minimal Data Relocation"* (PVLDB).

Tidehunter at its current stage requires some amount of manual configuration to work optimally.

Tidehunter provides a number of useful metrics (see below) that can be used to fine-tune configuration for best performance.

While it might not always provide the best performance out of the box, a fine-tuned Tidehunter can significantly surpass fine-tuned RocksDB for some workloads.  

# General concepts

* Tidehunter allows basic CRUD operations, and it allows point lookups as well as ordered iterators and range queries.
* A Tidehunter database can have one or more **key spaces**. A **key space** in Tidehunter is conceptually similar to column families in RocksDB or tables in SQL databases.
Each key space has a name and some parameters are configured per key space.
* Tidehunter shards key spaces internally, allowing for increased concurrency.
* Tidehunter supports write batches that apply atomically across different shards and different key spaces residing in the same Tidehunter database.
* When a Tidehunter database is created or opened, the full list of key spaces and their configuration must be provided.
* Tidehunter currently supports adding new key spaces to an existing database, but does not allow removing existing key spaces (though Tidehunter provides an API to quickly empty the contents of a key space).
* Some of the configurations for key spaces currently cannot be changed in an existing database.
* Note that currently Tidehunter works better with comparatively large values of 512 bytes and more.

# Key distribution

Unlike other key-value stores like RocksDB or sled, Tidehunter generally requires knowledge about the structure of the keys used in the key space.
The information about the key structure is passed to Tidehunter using **KeyType** and **KeyIndexing**.

KeyType provides Tidehunter with information about the statistical distribution of the keys.

Knowing the correct **KeyType** allows Tidehunter to efficiently shard internal indexes and reduce write amplification and access time.

Currently, Tidehunter works well with two types of keys:

* `KeyType::Uniform` can be used for keys that are uniformly distributed.
  * Examples of such keys are results of hash functions (SHA, Blake, etc.) or simply randomly sampled data from a uniform distribution.
  * As a concrete example, in blockchains, IDs of transactions, blocks, etc. are often the result of applying a hash function and therefore are uniformly distributed.
  * Uniform key type can be created via `KeyType::uniform(number_of_shards)`, where `number_of_shards` controls how many index shards are created.
  * A higher number of shards allows for more parallelism and lower write amplification, but some small amount of metadata is stored in memory for each shard. 
* `KeyType::PrefixUniform` are keys that have a non-uniform prefix of (relatively) low cardinality followed by a suffix.
  * Examples of such keys can be data identified by a sequence number (for example, block height) or prefixed by a sequence such as (Round, BlockHash).
  * Prefix-uniform keys are created via `KeyType::from_prefix_bits(bits)`, where `bits` is the bit length of the prefix.
  * The length of the prefix decides which portion of the key is treated as a shard ID—all keys sharing the same first prefix bits are stored in the same shard of the Tidehunter index.
  * Selecting too small a prefix can lead to high write amplification and inefficient queries.
  * Since each shard requires a small amount of in-memory metadata, selecting too large a prefix can lead to too many shards being created and memory pressure.
  * As practical guidance, let's say we want to store sequential 64-bit identifiers as keys. Selecting a 48-bit prefix would create shards each containing up to 2^(64-48) ≈ 64K keys. This is not too large to store in a single index but also does not create an excessive number of shard metadata entries.
  * As another example, if we use a 64-bit millisecond-precision timestamp as a key and roughly create one key per second, we can aim for the same 64K-sized shards by choosing a 38-bit prefix (since we create a key every second and the key has millisecond precision, the last ~10 bits of keys are roughly uniformly distributed).

You can double-check if the shard configuration is efficient by consulting the `max_index_size` metric, which reports the maximum size of the index per key space. The `entry_state` metric can be used to learn the current number of shards in a key space.

In addition to **KeyType**, **KeyIndexing** can be used to specify additional information that helps store keys efficiently:

* `KeyIndexing::Fixed(len)` should be used for keys that have a fixed length.
* `KeyIndexing::VariableLength` can be used if the key has variable length. This is less efficient than fixed-length keys, so it should not be used if the key length is known ahead of time.
* `KeyIndexing::Hash` can be used if the key does not follow any of the distributions outlined above. This will make Tidehunter hash the key before sharding in order to make the index keys uniformly distributed.
  * `KeyIndexing::Hash` only allows point lookups; iterators and range queries are not available with this indexing strategy.

# Metrics

Tidehunter exposes a number of metrics that can be useful for understanding performance bottlenecks and better tuning Tidehunter.

These metrics can be populated into a Prometheus registry using `Metrics::new_in`, and then exposed using a Prometheus HTTP endpoint.

You can check the list of all metrics in `tidehunter/src/metrics.rs`, but here are the most useful when debugging Tidehunter performance:

* `db_op` metric is a histogram reporting time in microseconds for database operations, including user-facing queries and internal background operations.
* `lookup_result` provides metrics outlining how `get` operations are served. This metric is broken down by key space, **result** (found/not found), and the **source** that served the result of the query:
  * `bloom` source indicates the result was served from the in-memory bloom filter.
  * `lru` source indicates the value was served from the in-memory LRU cache.
  * `cache` indicates the index lookup was served by the in-memory cache, but the value was looked up from the WAL (which can potentially incur a disk read for old values).
  * `lookup` indicates that an on-disk index lookup was used to find the WAL position for the value.
  * Ideally, you want to configure the bloom filter and LRU cache for your key space to minimize the `cache` and `lookup` sources, as those can be expensive.
* `wal_written_bytes_type` breaks down all disk writes performed by Tidehunter.
  * `record` and `tombstone` indicate user-initiated writes (updates and deletes, respectively).
  * `index` is a periodic internal write during index flush. If this value is too high, it might indicate the sharding configuration (**KeyType**) is not optimal. 

Long startup times can usually be debugged with the following metrics:

* `replayed_wal_records` indicates how many WAL records were replayed during startup from the latest snapshot. If this step is taking too long, adjusting `snapshot_written_bytes` and `snapshot_unload_threshold` can help.
* `large_table_init_mcs` indicates how much time is spent initializing the large table. Bloom filter initialization can be responsible if this time is too large.

If Tidehunter is using too much memory, the most helpful metrics to debug are:

* `value_cache_size` reports how much memory (in bytes) the value LRU cache takes per key space.
* `memory_estimate` provides the approximate size of other caches in bytes per key space:
  * `maps` reports how much memory is used by the recent memory map cache; this is directly configurable by the `max_maps` and `frag_size` configurations.
  * `bloom` reports the size of the bloom filter if configured for the key space.
  * `index_cache` reports the estimated size of the index_cache for the key space. This value is typically equal to `max_dirty_keys` × `length_of_the_key` × `number_of_shards` per key space. This estimate can be wildly inaccurate for key spaces with variable-length keys.
* `loaded_key_bytes` reports the actual current size occupied by the index cache. Note that the index cache can suffer from memory fragmentation and some other known inefficiencies, therefore actual memory usage can be several times higher than reported by this metric.

Rarer but still useful metrics that can indicate internal issues:
* `flush_pending` indicates the number of pending index flushes at a given time. If growing or very large, this can indicate that user writes outpace disk throughput.
* `large_table_contention` reports time in microseconds spent waiting to acquire the large table mutex. If the value is too large, this can indicate an insufficient number of large table shards, or that operations or keys are not being uniformly distributed across shards.
* `wal_write_wait` indicates user write operations are blocked because no writable fragments are allocated. This metric should be 0 during normal operation.
* `skip_stale_update` indicates the application performed concurrent updates of the same key and Tidehunter had to resolve a conflict. This is not an issue in general, but if this metric is too large, the application can benefit from optimizations that avoid concurrent overwrites of the same key.
* `flush_backpressure_count` indicates that some writes are stalled because of too many in-flight index flushes. This should be 0 during normal operation. If this metric is non-zero, this can significantly drive up tail latencies. This can be caused by an incorrectly selected `KeyType`. On very powerful machines, `max_flush_pending` might need to be increased. Setting `max_flush_pending` to 0 disables this backpressure mechanism. 

# Advanced concepts

Tidehunter has a couple of tricks up its sleeve that can provide very high speed for certain specialized operations:

* `Db::drop_cells_in_range` can be used to quickly drop a range of shards.
  * The update is applied immediately, but the disk space might be reclaimed at a later time.
  * This call takes a range of keys but requires it to fully overlap some range of shards; for example, you can't drop part of a shard with this method (see more details in the Rust documentation).
* `KeySpaceConfig::with_compactor` can be used to remove unused keys during index flushing. This is somewhat similar to RocksDB compaction filters.
* Tidehunter has a `Db::exists` check that is faster than reading the value via `get`.
* Tidehunter uses periodic index snapshots to speed up startup time. Normally, snapshots are created automatically, but if the application performs a large backfill, calling `Db::force_rebuild_control_region` after the backfill can be beneficial to reclaim disk space and reduce startup time. 

# Future work 

This section outlines things that are not currently supported but relatively easy to add in the near future:

* Sharding configuration(e.g. number of shards per key space) currently can not be changed, we will be likely allow increasing number of shards by the factor of power of two.
* Accessing small values(less than 512 bytes) can be currently less efficient with Tidehunter comparing to RocksDB because Tidehunter needs to do index lookup first and then read value from the WAL. This can be fixed by allowing to inline small values in the index.
* Tidehunter does not support merge operators, but they are conceptually compatible with current architecture
* Tidehunter does not use prefix coding when storing keys in the index. This means keys that have a frequent common prefixes are not stored efficiently. Tidehunter does support different index formats for different key spaces which leaves room for new prefix coded index format.

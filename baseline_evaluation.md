# Baseline System Evaluation Report

## Score Summary

| System | Recency | Academic | Relevance | Innovation | Code | Benchmarks | Ease of Comparison | **Total** |
|--------|---------|----------|-----------|------------|------|------------|-------------------|-----------|
| **LMDB** | 3 | 2 | 3 | 3 | 3 | 3 | 3 | **20/21** |
| **RocksDB** | 3 | 3 | 2 | 2 | 3 | 3 | 3 | **19/21** |
| **FASTER** | 3 | 3 | 2 | 3 | 3 | 3 | 2 | **19/21** |
| **Fjall** | 3 | 1 | 3 | 2 | 3 | 3 | 3 | **18/21** |
| **BlobDB** | 3 | 2 | 3 | 2 | 3 | 2 | 3 | **18/21** |
| **DiffKV** | 2 | 3 | 3 | 3 | 2 | 3 | 1 | **17/21** |
| **ADOC** | 2 | 3 | 2 | 3 | 2 | 3 | 2 | **17/21** |
| **SpanDB** | 2 | 3 | 3 | 3 | 2 | 3 | 1 | **17/21** |
| **Titan** | 3 | 1 | 3 | 2 | 3 | 2 | 2 | **16/21** |
| **TerarkDB** | 2 | 1 | 3 | 2 | 3 | 2 | 3 | **16/21** |
| **Bourbon** | 1 | 3 | 3 | 3 | 2 | 2 | 1 | **15/21** |
| **PebblesDB** | 1 | 3 | 2 | 3 | 2 | 3 | 1 | **15/21** |
| **SplinterDB** | 2 | 2 | 3 | 3 | 2 | 2 | 1 | **15/21** |
| **redb** | 3 | 1 | 2 | 1 | 3 | 2 | 3 | **15/21** |
| **KVell** | 2 | 3 | 2 | 3 | 2 | 2 | 1 | **15/21** |
| **HashKV** | 1 | 3 | 3 | 3 | 2 | 2 | 1 | **15/21** |
| **BVLSM** | 3 | 2 | 3 | 3 | 1 | 2 | 0 | **14/21** |
| **Monkey** | 1 | 3 | 2 | 2 | 2 | 2 | 1 | **13/21** |
| **Dostoevsky** | 1 | 3 | 2 | 2 | 2 | 2 | 1 | **13/21** |
| **AgateDB** | 2 | 1 | 2 | 1 | 2 | 1 | 3 | **12/21** |
| **CedrusDB** | 1 | 2 | 3 | 3 | 1 | 2 | 0 | **12/21** |

## Evaluation Criteria
Each system is scored 0-3 (0=Poor, 1=Acceptable, 2=Good, 3=Excellent) on:
1. **Recency & Maintenance** - How recent and actively maintained
2. **Academic Standing** - Paper quality and citations
3. **Technical Relevance** - Relevance to TideHunter's approach
4. **Core Innovation** - Uniqueness of approach
5. **Code Quality** - Implementation maturity and availability
6. **Benchmark Coverage** - Available performance data
7. **Ease of Comparison** - How easy to compare with TideHunter (Rust)
   - 3: Same language OR excellent Rust bindings available
   - 2: C/C++ with some bindings or reasonable FFI
   - 1: Requires significant wrapper/bridge work
   - 0: Very difficult (no implementation or incompatible)

---
## LMDB

### Summary
LMDB is a memory-mapped B-tree database with no WAL, using copy-on-write for ACID. Mature, widely used alternative architecture.

### Papers & Publications
- **Documentation**: [Official LMDB Documentation](http://www.lmdb.tech/doc/)
- **No formal paper**, but widely referenced in academic literature

### Technical Details
- **Architecture**: B+ tree, fully memory-mapped, no WAL
- **Language**: C (developed by Howard Chu at Symas Corporation)
- **Code**: http://www.lmdb.tech/doc/
- **Rust Bindings**: https://github.com/danburkert/lmdb-rs
- **Latest Version**: 0.9.33 (May 2024)
- **Key Innovation**: Shadow paging, zero-copy reads, copy-on-write

### Value Size Optimization
- **Good for moderate-sized values** - Zero-copy benefits for large reads, but overflow pages for values >2KB waste space
- Key size limited to 511 bytes by default (can be recompiled up to ~2KB)
- Values >1KB go to overflow pages, wasting ~1KB on average
- Maximum value size: 4GB
- Best performance with values that fit efficiently in B+ tree pages

### Scores
1. **Recency & Maintenance**: 3 (Active maintenance)
2. **Academic Standing**: 2 (Industry standard, no paper)
3. **Technical Relevance**: 3 (Memory-mapped like TideHunter)
4. **Core Innovation**: 3 (No WAL approach)
5. **Code Quality**: 3 (Production quality)
6. **Benchmark Coverage**: 3 (Well benchmarked)
7. **Ease of Comparison**: 3 (Excellent lmdb-rs bindings available, well-maintained)

**Total Score: 20/21**

### Recommendation
**Essential baseline** - Perfect contrast showing pure memory-mapped without WAL.

---

## RocksDB

### Summary
RocksDB is Facebook/Meta's production LSM-tree key-value store, forked from LevelDB in 2012. It's the industry standard baseline for KV store comparisons, with extensive production deployment at scale.

### Papers & Publications
- **Main Paper**: ["RocksDB: Evolution of Development Priorities"](https://dl.acm.org/doi/10.1145/3483840) (FAST'21/ACM TOS 2021)
- **Workload Paper**: ["Characterizing, Modeling, and Benchmarking RocksDB Key-Value Workloads at Facebook"](https://www.usenix.org/system/files/fast20-cao_zhichao.pdf) (FAST'20)
- **Citations**: Extremely high (1000+ citations combined)

### Technical Details
- **Architecture**: LSM-tree with tiered compaction
- **Language**: C++ with bindings for many languages
- **Code**: https://github.com/facebook/rocksdb
- **Rust Bindings**: https://github.com/rust-rocksdb/rust-rocksdb
- **Latest Version**: 9.11.2 (actively maintained, v10.x released in 2024)
- **Key Features**: Multi-threaded compaction, column families, transactions, BlobDB integration

### Performance Characteristics
- **Benchmarks**: Extensive (db_bench tool, YCSB results available)
- **In-memory**: 4.5-7M QPS for point lookups
- **Flash Storage**: Optimized for SSDs
- **Write Amplification**: Focus of early optimization efforts

### Value Size Optimization
- **Poor for large values without BlobDB** - High write amplification from compaction
- Native RocksDB copies large values repeatedly during each compaction level
- Performance degrades significantly with values >10KB
- Consider using BlobDB integration for large value workloads

### Scores
1. **Recency & Maintenance**: 3 (Very active, latest release Dec 2024)
2. **Academic Standing**: 3 (Multiple high-quality papers, industry standard)
3. **Technical Relevance**: 2 (LSM vs TideHunter's WAL, but common baseline)
4. **Core Innovation**: 2 (Mature, well-understood design)
5. **Code Quality**: 3 (Production-grade, widely deployed)
6. **Benchmark Coverage**: 3 (Extensive benchmarks available)
7. **Ease of Comparison**: 3 (Excellent rust-rocksdb crate, well-maintained bindings)

**Total Score: 19/21**

### Pros for TideHunter Comparison
- Industry standard everyone understands
- Extensive benchmark data available
- Clear architectural differences (LSM vs WAL)
- Production-proven at scale

### Cons for TideHunter Comparison
- Very different architecture (LSM vs WAL+memory-mapped)
- Not optimized for large values without BlobDB
- May not highlight TideHunter's unique strengths

### Recommendation
**Essential baseline** - Must include as the industry standard reference point.

---

## FASTER

### Summary
FASTER (SIGMOD'18) from Microsoft Research achieves 160M ops/sec using hybrid log spanning memory and storage with epoch protection framework.

### Papers & Publications
- **Main Paper**: ["FASTER: A Concurrent Key-Value Store with In-Place Updates"](https://dl.acm.org/doi/10.1145/3183713.3196898) (SIGMOD'18)
- **Authors**: Badrish Chandramouli, Guna Prasaad, Donald Kossmann, Justin Levandoski, James Hunter, Mike Barnett
- **Code**: https://github.com/microsoft/FASTER
- **Rust Wrapper**: https://github.com/faster-rs/faster-rs (experimental)
- **Follow-up**: Multiple papers, active research

### Technical Details
- **Architecture**: Hybrid log (memory + storage), concurrent hash index
- **Language**: C# and C++
- **Key Innovation**: Epoch protection, in-place updates in memory
- **Features**: Latch-free design, read-copy-update strategy

### Value Size Optimization
- **Handles both small and large values well** - Variable-length support with SpanByte
- Partial updates for large values avoid copying entire entries
- Hot data updated in-place, cold data uses read-copy-update
- Optimized for both memory-resident and larger-than-memory datasets
- Hybrid log design efficiently manages temporal locality

### Performance & Scores
1. **Recency & Maintenance**: 3 (Active Microsoft project)
2. **Academic Standing**: 3 (SIGMOD + follow-ups)
3. **Technical Relevance**: 2 (Different concurrency model)
4. **Core Innovation**: 3 (Epoch protection framework)
5. **Code Quality**: 3 (Microsoft quality)
6. **Benchmark Coverage**: 3 (Extensive benchmarks)
7. **Ease of Comparison**: 2 (C#/C++ with experimental faster-rs Rust wrapper)

**Total Score: 19/21**

### Recommendation
**Strong baseline for concurrency** - Excellent for comparing concurrent access patterns and in-memory performance.

---

## DiffKV

### Summary
DiffKV (USENIX ATC'21) uses differentiated KV storage with sorted keys but partially-sorted values. State-of-the-art academic KV separation.

### Papers & Publications
- **Paper**: ["Differentiated Key-Value Storage Management for Balanced I/O Performance"](https://www.usenix.org/system/files/atc21-li-yongkun.pdf) (USENIX ATC'21)
- **Authors**: Yongkun Li, Zhen Liu, Patrick P. C. Lee, Jiayu Wu, Yinlong Xu, Yi Wu, Liu Tang, Qi Liu, Qiu Cui
- **Code**: https://github.com/ustcadsl/diffkv

### Technical Details
- **Architecture**: Differentiated ordering for keys vs values
- **Key Innovation**: Fine-grained KV separation by size

### Scores
1. **Recency & Maintenance**: 2 (2021)
2. **Academic Standing**: 3 (USENIX ATC)
3. **Technical Relevance**: 3 (KV separation)
4. **Core Innovation**: 3 (Differentiated management)
5. **Code Quality**: 2 (Research code)
6. **Benchmark Coverage**: 3 (Extensive comparisons)
7. **Ease of Comparison**: 1 (C++ based on Titan, no Rust bindings)

**Total Score: 17/21**

### Recommendation
**Strong academic baseline** - Latest KV separation research, good for large value comparison.

---

## ADOC

### Summary
ADOC (FAST'23) is an automatic tuning framework for RocksDB that addresses write stalls by identifying and controlling "data overflow" - rapid expansion of components due to data flow surges. Achieves dramatic performance improvements through automatic thread and batch size tuning.

### Papers & Publications
- **Main Paper**: ["ADOC: Automatically Harmonizing Dataflow Between Components in Log-Structured Key-Value Stores for Improved Performance"](https://www.usenix.org/conference/fast23/presentation/yu) (FAST'23)
- **Authors**: Jinghuan Yu (City University of Hong Kong), Sam H. Noh (UNIST & Virginia Tech), Young-ri Choi (UNIST), Chun Jason Xue (City University of Hong Kong)
- **Citations**: Recent paper, citation count growing

### Technical Details
- **Architecture**: Online tuning framework for RocksDB
- **Language**: C++ (modification of RocksDB)
- **Code**: https://github.com/supermt/FEAT_7.11
- **Key Innovation**: Automatic data overflow control through dynamic thread pool and batch size tuning
- **Features**: ADOC-T (thread tuning), ADOC-B (batch tuning), or combined mode

### Performance Characteristics
- **Write Stall Reduction**: 87.9% reduction in write stall duration
- **Throughput**: 322.8% improvement vs auto-tuned RocksDB
- **vs SILK**: 66% higher throughput for write-intensive workloads
- **Memory**: 20% less DRAM usage than SILK

### Value Size Optimization
- **Indifferent to value size** - Focuses on tuning thread pools and batch sizes
- Addresses write stalls regardless of value size
- Performance improvements apply across all value ranges
- Automatic tuning adapts to workload characteristics
- Benefits both small and large value workloads equally

### Scores
1. **Recency & Maintenance**: 2 (2023 paper, research implementation)
2. **Academic Standing**: 3 (FAST'23, top-tier venue)
3. **Technical Relevance**: 2 (LSM optimization, different from WAL focus)
4. **Core Innovation**: 3 (Novel automatic tuning approach)
5. **Code Quality**: 2 (Research implementation available)
6. **Benchmark Coverage**: 3 (Extensive benchmarks vs RocksDB and SILK)
7. **Ease of Comparison**: 2 (Modified RocksDB, can use rust-rocksdb bindings)

**Total Score: 17/21**

### Recommendation
**Important for write-heavy workloads** - Shows state-of-the-art in automatic LSM tuning, dramatic write stall reduction.

---

## SpanDB

### Summary
SpanDB (FAST'21) is a hybrid storage KV store that relocates WAL and top LSM levels to fast NVMe SSDs while keeping bulk data on cheaper storage. Uses SPDK for high-performance parallel I/O. Highly relevant due to WAL optimization focus.

### Papers & Publications
- **Main Paper**: ["SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"](https://www.usenix.org/conference/fast21/presentation/chen-hao) (FAST'21)
- **Authors**: Hao Chen, Chaoyi Ruan, Cheng Li, Xiaosong Ma, Yinlong Xu
- **Citations**: 60 citations, 10 highly influential (Semantic Scholar)

### Technical Details
- **Architecture**: Hybrid storage with selective SSD deployment
- **Language**: C++ with SPDK
- **Code**: https://github.com/SpanDB/SpanDB
- **Key Innovation**: WAL on fast device, TopFS stripped-down filesystem
- **Features**: Parallel WAL writes via SPDK, asynchronous request processing

### Performance Characteristics
- **Throughput**: 8.8× improvement over RocksDB
- **Latency**: 9.5-58.3% reduction vs RocksDB
- **vs KVell**: 96-140% throughput with 2.3-21.6× lower latency
- **Migration**: Automatic migration from existing RocksDB

### Value Size Optimization
- **General purpose, not value-size specific** - Focuses on WAL/LSM placement on fast storage
- Benefits all value sizes through hybrid storage approach
- No specific large value optimizations or key-value separation
- Performance gains come from storage tiering rather than value handling

### Scores
1. **Recency & Maintenance**: 2 (2021 paper, maintained repository)
2. **Academic Standing**: 3 (FAST'21, 60 citations, influential)
3. **Technical Relevance**: 3 (WAL optimization very relevant)
4. **Core Innovation**: 3 (Novel hybrid storage with SPDK)
5. **Code Quality**: 2 (Research code, requires SPDK setup)
6. **Benchmark Coverage**: 3 (Extensive benchmarks)
7. **Ease of Comparison**: 1 (C++ with SPDK, complex setup, no Rust bindings)

**Total Score: 17/21**

### Recommendation
**Highly relevant for WAL comparison** - Direct competitor with WAL optimization on fast storage, similar goals to TideHunter.

---

## BlobDB (RocksDB Integrated)

### Summary
BlobDB is RocksDB's integrated key-value separation feature. There are two implementations: legacy (StackableDB-based) and integrated (2020-2021). The integrated version implements WiscKey's concepts for handling large values by storing them separately from the LSM tree.

### Papers & Publications
- **Blog Post**: ["Integrated BlobDB"](https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html) (RocksDB Blog, May 2021)
- **Based on**: [WiscKey paper](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) (FAST'16) concepts
- **Parent Papers**: Same as RocksDB (FAST'20, FAST'21)

### Technical Details
- **Architecture**: Key-value separation within RocksDB
- **Language**: C++ (part of RocksDB)
- **Code**: Part of https://github.com/facebook/rocksdb (enable_blob_files option)
- **Integration**: Fully integrated since RocksDB 6.18+ (enable_blob_files = true)
- **Key Features**: Blob files, integrated GC during compaction, configurable thresholds
- **Note**: Legacy BlobDB::Open() method may be deprecated; use enable_blob_files instead

### Performance Characteristics
- **Value Sizes Tested**: 1KB to 1MB
- **Benchmarks**: Tested on 18-core Skylake, 64GB RAM, NVMe SSDs
- **Write Amplification**: Significantly reduced for large values
- **GC Efficiency**: Better than WiscKey (no Get+Put needed)

### Value Size Optimization
- **Specifically designed for large values** - Key-value separation for values >min_blob_size
- 2.1-3.5x higher throughput for large values vs standard RocksDB
- Better performance starts at ~1KB values, optimal >10KB
- Integrated GC during compaction handles blob files efficiently
- Trade-off: Some space amplification from unreferenced blobs

### Scores
1. **Recency & Maintenance**: 3 (Part of active RocksDB development)
2. **Academic Standing**: 2 (Implementation of academic concept, no separate paper)
3. **Technical Relevance**: 3 (Key-value separation very relevant to TideHunter)
4. **Core Innovation**: 2 (Implementation of existing concept, but well-executed)
5. **Code Quality**: 3 (Production-grade, part of RocksDB)
6. **Benchmark Coverage**: 2 (Good benchmarks but not as extensive as standalone systems)
7. **Ease of Comparison**: 3 (Same rust-rocksdb bindings, enable_blob_files option)

**Total Score: 18/21**

### Pros for TideHunter Comparison
- Direct competitor for large value handling
- Key-value separation vs TideHunter's approach
- Production-proven implementation
- Same API as RocksDB (easy switching)

### Cons for TideHunter Comparison
- Not a standalone system (part of RocksDB)
- Less architectural diversity (still LSM-based)
- May overshadow pure RocksDB comparison

### Recommendation
**Essential for large values** - Must include when comparing large value performance. Use RocksDB with BlobDB enabled for large value workloads.

---

## Fjall

### Summary
Fjall is a modern, production-grade LSM-tree storage engine written in pure Rust with built-in key-value separation for large values. Very actively maintained with v2.8 released in March 2025.

### Papers & Publications
- **No formal paper** - Industry/open-source project
- **Documentation**: https://fjall-rs.github.io/
- **Repository**: https://github.com/fjall-rs/fjall
- **Blog Posts**: Regular technical posts documenting architecture and improvements

### Technical Details
- **Architecture**: LSM-tree with optional key-value separation via value log
- **Language**: Pure Rust (100% memory safe)
- **Code**: https://github.com/fjall-rs/fjall
- **Latest Version**: 2.8 (March 2025)
- **Key Features**: MVCC, transactions, partitions (column families), blob storage

### Performance Characteristics
- **Value Limits**: Keys up to 65KB, values up to 4GB
- **Concurrency**: Single-writer and multi-writer transactions
- **Optimization**: Continuous performance improvements (2.3 → 2.8)
- **Memory**: Efficient caching with configurable limits

### Value Size Optimization
- **Built-in key-value separation** - Automatic blob storage for large values
- Configurable blob threshold (default optimized for SSD)
- Blob values stored in separate log files to reduce write amplification
- Size metadata stored in index for efficient space queries
- Optimized for mixed workloads with both small and large values

### Scores
1. **Recency & Maintenance**: 3 (Very active, v2.8 in March 2025)
2. **Academic Standing**: 1 (No paper, open-source project)
3. **Technical Relevance**: 3 (Key-value separation, LSM-tree)
4. **Core Innovation**: 2 (Well-executed but not novel)
5. **Code Quality**: 3 (Production-grade Rust)
6. **Benchmark Coverage**: 3 (Comprehensive benchmarks)
7. **Ease of Comparison**: 3 (Pure Rust, native integration)

**Total Score: 18/21**

### Pros for TideHunter Comparison
- Pure Rust eliminates FFI overhead
- Modern implementation with latest techniques
- Active development ensures bug fixes
- Direct API compatibility for benchmarking

### Cons for TideHunter Comparison
- No academic paper for citations
- Still LSM-based (not fundamentally different)

### Recommendation
**Best Rust baseline** - Essential for fair Rust-to-Rust performance comparison without FFI overhead.

---

## PebblesDB

### Summary
PebblesDB is a write-optimized key-value store from UT Austin that uses Fragmented Log-Structured Merge Trees (FLSM). Presented at SOSP 2017, it achieves 6.7x write throughput of RocksDB with 2.4-3x lower write amplification.

### Papers & Publications
- **Main Paper**: ["PebblesDB: Building Key-Value Stores using FLSM"](https://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf) (SOSP'17)
- **Authors**: Pandian Raju, Rohan Kadekodi, Vijay Chidambaram, Ittai Abraham
- **Citations**: Well-cited academic paper (~200+ citations)

### Technical Details
- **Architecture**: FLSM (Fragmented LSM with guards, Skip List inspired)
- **Language**: C++ (built on HyperLevelDB/LevelDB)
- **Code**: https://github.com/utsaslab/pebblesdb
- **Compatibility**: Drop-in replacement for LevelDB/HyperLevelDB
- **Key Innovation**: Guards to avoid rewriting data in same level

### Performance Characteristics
- **Write Throughput**: 6.7x better than RocksDB
- **Write Amplification**: 2.4-3x reduction vs RocksDB
- **Read Performance**: Comparable to HyperLevelDB
- **Range Queries**: Penalty on small ranges when fully compacted
- **YCSB Results**: 18-105% throughput increase in MongoDB/HyperDex

### Value Size Optimization
- **Write-optimized regardless of value size** - FLSM benefits apply to all sizes
- No specific value size optimizations or key-value separation
- General write amplification reduction helps all value ranges
- Guards in FLSM reduce rewriting for all data sizes
- Performance gains are value-size agnostic

### Scores
1. **Recency & Maintenance**: 1 (2017 project, unclear if actively maintained)
2. **Academic Standing**: 3 (SOSP paper, well-cited)
3. **Technical Relevance**: 2 (LSM variant, different from TideHunter)
4. **Core Innovation**: 3 (Novel FLSM approach)
5. **Code Quality**: 2 (Research code, C++, works but not production-grade)
6. **Benchmark Coverage**: 3 (Extensive YCSB benchmarks published)
7. **Ease of Comparison**: 1 (C++ based on LevelDB, no Rust bindings, requires FFI)

**Total Score: 15/21**

### Pros for TideHunter Comparison
- Strong write optimization focus
- Novel approach to reducing write amplification
- Good academic baseline with clear benchmarks
- Different LSM strategy to contrast

### Cons for TideHunter Comparison
- May not be actively maintained
- Still LSM-based (not fundamentally different)
- Research quality code vs production systems
- Not specifically optimized for large values

### Recommendation
**Good academic baseline** - Consider if you want to show TideHunter vs novel LSM variants. Strong for write-heavy workload comparisons.

---

## Titan

### Summary
Titan is PingCAP's production key-value separation plugin for RocksDB, used in TiKV/TiDB. It achieves up to 6x performance improvement for large values. Default-enabled in TiDB v7.6.0+ for NEW clusters only (existing clusters retain old settings).

### Papers & Publications
- **Blog Post**: ["Titan: A RocksDB Plugin to Reduce Write Amplification"](https://www.pingcap.com/blog/titan-storage-engine-design-and-implementation/) (PingCAP)
- **No formal paper**: Industrial/engineering project
- **Referenced in**: [DiffKV](https://www.usenix.org/system/files/atc21-li-yongkun.pdf) (USENIX ATC'21) for comparison

### Technical Details
- **Architecture**: RocksDB plugin with key-value separation
- **Language**: C++
- **Code**: https://github.com/tikv/titan
- **Integration**: Plugin for RocksDB, used in TiKV
- **Key Features**: WAL integration, blob files generated during flush

### Performance Characteristics
- **Large Values**: 6x improvement for 32KB values
- **Medium Values**: 2x improvement for 1KB values
- **Default Threshold**: min-blob-size changed from 1KB to 32KB in TiDB 7.6.0+
- **Production Tested**: Handles TiDB workloads at scale

### Value Size Optimization
- **Optimized for large values** - Key-value separation for values >32KB (default)
- 6x improvement for 32KB values, 2x for 1KB values
- Worse compression ratio than RocksDB for values <32KB (30-50% larger)
- Sweet spot at 32KB threshold balances performance and storage
- During compaction, values >min-blob-size separated to blob files

### Scores
1. **Recency & Maintenance**: 3 (Active development, default in TiDB 7.6+)
2. **Academic Standing**: 1 (No paper, but referenced in academic work)
3. **Technical Relevance**: 3 (Key-value separation directly relevant)
4. **Core Innovation**: 2 (WiscKey-inspired but production-hardened)
5. **Code Quality**: 3 (Production-grade, used in TiKV)
6. **Benchmark Coverage**: 2 (Good real-world data, less academic benchmarks)
7. **Ease of Comparison**: 2 (RocksDB plugin in C++, requires complex setup with rust-rocksdb)

**Total Score: 16/21**

### Pros for TideHunter Comparison
- Production-tested key-value separation
- Excellent for large value comparison
- Real-world deployment data
- Active maintenance and development

### Cons for TideHunter Comparison
- No formal academic paper
- Still RocksDB-based (LSM architecture)
- Less detailed public benchmarks than academic systems

### Recommendation
**Strong practical baseline** - Excellent choice for large value comparisons. Production-proven alternative to BlobDB with real deployment data.

---

## TerarkDB

### Summary
TerarkDB is ByteDance's RocksDB replacement with optimized tail latency, throughput and compression. Production-tested at ByteDance scale with specific optimizations for large-scale data storage.

### Papers & Publications
- **No formal paper** - Industrial project from ByteDance
- **Repository**: https://github.com/bytedance/terarkdb
- **Based on**: RocksDB v5.18.3 fork
- **Referenced in**: DumpKV paper (VLDB 2024) and other academic works

### Technical Details
- **Architecture**: LSM-tree with v-SSTable for value storage
- **Language**: C++ (83.3% of codebase)
- **Code**: https://github.com/bytedance/terarkdb
- **Latest Version**: v1.3.6 (January 2021)
- **Key Features**: TerarkZipTable, optimized compression, reduced tail latency

### Performance Characteristics
- **Latency**: Reduced tail latency spikes vs RocksDB
- **Throughput**: "Tremendous" improvement claimed
- **Compression**: Better compression ratios with TerarkZipTable
- **Production**: Deployed at ByteDance scale

### Value Size Optimization
- **v-SSTable mechanism** - Values stored in special SSTable format
- Stores file number in value index pointer (not offset)
- Values in v-SSTable stored in sorted order
- Maintains inheritance map between v-SSTables for lookups
- Optimized for ByteDance's large-scale workloads

### Scores
1. **Recency & Maintenance**: 2 (Last update 2021, production use)
2. **Academic Standing**: 1 (No paper, industry project)
3. **Technical Relevance**: 3 (v-SSTable for large values)
4. **Core Innovation**: 2 (RocksDB optimization)
5. **Code Quality**: 3 (Production at ByteDance)
6. **Benchmark Coverage**: 2 (Good benchmarks available)
7. **Ease of Comparison**: 3 (RocksDB-compatible API)

**Total Score: 16/21**

### Pros for TideHunter Comparison
- Production-tested at massive scale
- RocksDB-compatible for easy integration
- Specific large value optimizations
- Real-world deployment validation

### Cons for TideHunter Comparison
- No academic paper
- Less actively maintained (2021)
- Still LSM-based architecture

### Recommendation
**Production alternative to RocksDB** - Good for showing TideHunter vs production-optimized LSM.

---

## Bourbon

### Summary
Bourbon (OSDI 2020) uses learned indexes to accelerate LSM-tree lookups. Built on WiscKey, it employs greedy piecewise linear regression to learn key distributions for 1.23x-1.78x faster lookups.

### Papers & Publications
- **Main Paper**: ["From WiscKey to Bourbon: A Learned Index for Log-Structured Merge Trees"](https://www.usenix.org/conference/osdi20/presentation/dai) (OSDI'20)
- **Authors**: Yifan Dai, Yien Xu, Aishwarya Ganesan, Ramnatthan Alagappan, Brian Kroth, Andrea & Remzi Arpaci-Dusseau
- **Code**: https://github.com/edydfang/Bourbon

### Technical Details
- **Architecture**: LSM-tree with learned indexes, based on WiscKey
- **Language**: C++ (94% of codebase)
- **Key Innovation**: Piecewise linear regression for key distribution
- **Features**: Cost-benefit analyzer for selective learning

### Performance Characteristics
- **Lookup improvement**: 1.23x-1.78x vs state-of-the-art
- **Based on**: WiscKey (20K LOC) + 5K LOC for Bourbon
- **Learning**: Per-file models built after creation

### Value Size Optimization
- **Inherited from WiscKey** - Full key-value separation
- Learned indexes accelerate key lookups in LSM tree
- Values stored separately in value log
- Benefits compound: faster key lookup + reduced write amplification
- Particularly effective for read-heavy workloads with large values

### Scores
1. **Recency & Maintenance**: 1 (2020, research prototype)
2. **Academic Standing**: 3 (OSDI paper)
3. **Technical Relevance**: 3 (Learned indexes + KV separation)
4. **Core Innovation**: 3 (Novel learned index approach)
5. **Code Quality**: 2 (Research implementation)
6. **Benchmark Coverage**: 2 (Academic benchmarks)
7. **Ease of Comparison**: 1 (C++, no Rust bindings)

**Total Score: 15/21**

### Pros for TideHunter Comparison
- Innovative learned index approach
- Strong academic pedigree (OSDI)
- Combines two important optimizations

### Cons for TideHunter Comparison
- Not actively maintained
- Research-quality code
- Complex C++ integration needed

### Recommendation
**Interesting for learned index comparison** - Shows alternative optimization approach to traditional indexing.

---

## KVell

### Summary
KVell (SOSP'19) is a shared-nothing KV store designed for NVMe SSDs. It achieves close-to-device bandwidth by avoiding sorting and synchronization, using "sharing is harder than send" philosophy.

### Papers & Publications
- **Main Paper**: ["KVell: the Design and Implementation of a Fast Persistent KV Store"](https://dl.acm.org/doi/10.1145/3341301.3359628) (SOSP'19)
- **Authors**: Baptiste Lepers, Oana Balmau, Karan Gupta, Willy Zwaenepoel
- **Code**: https://github.com/BLepers/KVell

### Technical Details
- **Architecture**: Shared-nothing, no data sorting on disk
- **Language**: C
- **Key Innovation**: Maximizes NVMe bandwidth, avoids LSM/B-tree overhead
- **Features**: Per-CPU slices, batched I/O, in-memory B-tree index

### Value Size Optimization
- **4KB threshold for atomic writes** - Special handling for values >4KB
- Fixed slab sizes: 100B to 4KB with padding for smaller items
- Items >4KB cannot guarantee atomic writes, use timestamps for recovery
- Optimized for NVMe bandwidth regardless of value size
- Shared-nothing design benefits all value sizes equally

### Performance & Scores
1. **Recency & Maintenance**: 2 (2019, unclear maintenance)
2. **Academic Standing**: 3 (SOSP paper)
3. **Technical Relevance**: 2 (Different from WAL approach)
4. **Core Innovation**: 3 (Novel NVMe optimization)
5. **Code Quality**: 2 (Research code available)
6. **Benchmark Coverage**: 2 (Academic benchmarks)
7. **Ease of Comparison**: 1 (C implementation, no Rust bindings, requires FFI wrapper)

**Total Score: 15/21**

### Recommendation
**Interesting for NVMe comparison** - Shows maximum device utilization approach.

---

## HashKV

### Summary
HashKV (USENIX ATC'18) uses hash-based data grouping for efficient updates in KV separation. Achieves 4.6x update throughput and 53.4% less write traffic vs standard KV separation.

### Papers & Publications
- **Main Paper**: ["HashKV: Enabling Efficient Updates in KV Storage via Hashing"](https://www.usenix.org/conference/atc18/presentation/chan) (USENIX ATC'18)
- **Authors**: Helen H. W. Chan, Yongkun Li, Patrick P. C. Lee, Yinlong Xu
- **Extended**: [ACM TOS 2019](https://dl.acm.org/doi/10.1145/3340287)
- **Code**: https://github.com/Legend147/HashKV

### Technical Details
- **Architecture**: Hash-based data grouping for value storage
- **Language**: C++
- **Key Innovation**: Deterministic value mapping for efficient GC
- **Features**: Dynamic allocation, hotness awareness

### Performance & Scores
1. **Recency & Maintenance**: 1 (2018, likely inactive)
2. **Academic Standing**: 3 (USENIX ATC + journal)
3. **Technical Relevance**: 3 (Update optimization relevant)
4. **Core Innovation**: 3 (Novel hash-based grouping)
5. **Code Quality**: 2 (Research prototype)
6. **Benchmark Coverage**: 2 (Good experimental data)
7. **Ease of Comparison**: 1 (C++ implementation, no Rust bindings available)

**Total Score: 15/21**

### Recommendation
**Good for update-intensive workloads** - Relevant if focusing on update performance.

### Value Size Optimization
- **Designed for key-value separation with large values** - Uses selective KV separation
- Keeps small-size KV pairs in entirety in the LSM-tree to simplify lookups
- Larger values stored separately with hash-based data grouping
- Optimized GC for efficiently handling large values in update-heavy workloads
- Achieves 53.4% less write traffic vs standard KV separation
- Best for workloads with mixed value sizes and frequent updates

---


## Monkey

### Summary
Monkey (SIGMOD'17) optimizes LSM-tree design space navigation with optimal Bloom filter allocation. Academic project focusing on read-write trade-offs.

### Papers & Publications
- **Main Paper**: ["Monkey: Optimal Navigable Key-Value Store"](https://dl.acm.org/doi/10.1145/3035918.3064054) (SIGMOD'17)
- **Authors**: Niv Dayan, Manos Athanassoulis, Stratos Idreos
- **Code**: https://github.com/nitzancarmi/monkey

### Scores
1. **Recency & Maintenance**: 1
2. **Academic Standing**: 3
3. **Technical Relevance**: 2
4. **Core Innovation**: 2
5. **Code Quality**: 2
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 1 (C++/Java implementation, no Rust bindings)

**Total Score: 13/21**

### Recommendation
**Skip** - LSM optimization less relevant to TideHunter.

### Value Size Optimization
- **Value size agnostic** - Focuses on Bloom filter optimization rather than value handling
- No key-value separation mechanism
- Tested with uniform 16-byte entries in experiments
- Optimizes read-write trade-offs independent of value size
- Performance gains (50-90% lookup cost reduction) apply to all value sizes
- Better suited for workloads with predictable, moderate-sized values

---

## Dostoevsky

### Summary
Dostoevsky (SIGMOD'18) uses lazy leveling to optimize LSM-trees for write-heavy workloads. Academic project from Harvard/Stratos.

### Papers & Publications
- **Main Paper**: ["Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores via Adaptive Removal of Superfluous Merging"](https://dl.acm.org/doi/10.1145/3183713.3196927) (SIGMOD'18)
- **Authors**: Niv Dayan, Stratos Idreos
- **Code**: https://github.com/BU-DiSC/dostoevsky-tree
- **Key Innovation**: Lazy leveling and Fluid LSM-tree

### Scores
1. **Recency & Maintenance**: 1
2. **Academic Standing**: 3
3. **Technical Relevance**: 2
4. **Core Innovation**: 2
5. **Code Quality**: 2
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 1 (C++ implementation, no Rust bindings)

**Total Score: 13/21**

### Recommendation
**Skip** - Another LSM variant, limited relevance.

### Value Size Optimization
- **Value size agnostic** - Focuses on merge policy optimization, not value handling
- No key-value separation mechanism
- Lazy Leveling and Fluid LSM apply equally to all value sizes
- Implemented on top of RocksDB, inherits its value handling
- Performance improvements from reduced merge operations benefit all value sizes
- Best for write-heavy workloads regardless of value size

---

## AgateDB

### Summary
AgateDB is an experimental persistent KV store written in pure Rust, designed for the TiKV project. Port of badger (Go) to Rust with planned optimizations from unistore. Notable as a pure Rust implementation for direct comparison.

### Papers & Publications
- **No formal paper** - Experimental project
- **Repository**: https://github.com/tikv/agatedb
- **Related**: Part of TiKV ecosystem development

### Technical Details
- **Architecture**: LSM-based, port of badger with MVCC support
- **Language**: Pure Rust (like TideHunter!)
- **Code**: https://github.com/tikv/agatedb (develop branch for latest)
- **Key Features**: Memory safety, designed for TiKV integration
- **Status**: Early heavy development, experimental

### Scores
1. **Recency & Maintenance**: 2 (Active development but experimental)
2. **Academic Standing**: 1 (No paper, experimental project)
3. **Technical Relevance**: 2 (Different architecture but Rust)
4. **Core Innovation**: 1 (Port of existing system)
5. **Code Quality**: 2 (Experimental, not production ready)
6. **Benchmark Coverage**: 1 (No published benchmarks)
7. **Ease of Comparison**: 3 (Pure Rust implementation!)

**Total Score: 12/21**

### Recommendation
**Interesting for Rust comparison** - While experimental, valuable as pure Rust baseline for language-specific comparisons.

### Value Size Optimization
- **Optimized for large values** - Port of BadgerDB with key-value separation
- Uses value log (vLog) for large values, likely with 4KB threshold (BadgerDB default)
- Small values (<threshold) stored directly in LSM tree
- Large values stored in separate value log files
- Stores only <key, <fileno, offset>> in LSM tree for large values
- Reduces write amplification for large value workloads
- Best for mixed workloads with many values >4KB

---

## SplinterDB

### Summary
SplinterDB from VMware uses size-tiered Bε-trees instead of LSM-trees. Originally presented at USENIX ATC 2020, open-sourced in 2022. Achieves 6-10x faster insertions than RocksDB.

### Papers & Publications
- **Main Paper**: ["SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores"](https://www.usenix.org/conference/atc20/presentation/conway) (USENIX ATC'20)
- **Blog**: ["Introducing SplinterDB"](https://blogs.vmware.com/opensource/2022/06/15/introducing-splinterdb-high-performing-key-value-store/) (VMware Open Source Blog, 2022)
- **Code**: https://github.com/vmware/splinterdb

### Technical Details
- **Architecture**: Bε-tree (not LSM)
- **Language**: C
- **Performance**: 6-10x insertion speed, 1.5-2.6x query speed vs RocksDB

### Value Size Optimization
- **Optimized for small values** - Best with small key-value pairs in memory-constrained environments
- Key size: 8-105 bytes, values must fit in 4KB page
- Designed for stringent requirements: small KV pairs and restricted memory
- Less advantage with large values or plentiful memory
- 2x lower write amplification than RocksDB for small values

### Scores
1. **Recency & Maintenance**: 2 (2022, VMware project)
2. **Academic Standing**: 2 (Industry project)
3. **Technical Relevance**: 3 (Different tree structure)
4. **Core Innovation**: 3 (Bε-tree for KV)
5. **Code Quality**: 2 (VMware quality)
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 1 (C implementation, no Rust bindings)

**Total Score: 15/21**

### Recommendation
**Interesting alternative** - Shows non-LSM approach to KV stores.

---


## CedrusDB

### Summary
CedrusDB (2020) uses memory-mapped lazy-trie with WAL, achieving near-optimal height. Similar hybrid approach to TideHunter.

### Papers & Publications
- **Paper**: ["CedrusDB: Persistent Key-Value Store with Memory-Mapped Lazy-Trie"](https://arxiv.org/abs/2005.13762) (arXiv 2020, updated through July 2021)
- **Authors**: Maofan Yin, Hongbo Zhang, Robbert van Renesse, Emin Gün Sirer
- **Code**: Not publicly available

### Scores
1. **Recency & Maintenance**: 1 (2020, unclear status)
2. **Academic Standing**: 2 (arXiv paper)
3. **Technical Relevance**: 3 (WAL + memory-mapped)
4. **Core Innovation**: 3 (Lazy-trie structure)
5. **Code Quality**: 1 (Unknown availability)
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 0 (No known implementation available)

**Total Score: 12/21**

### Recommendation
**Conceptually interesting** - Similar hybrid approach but likely hard to obtain/run.

### Value Size Optimization
- **Value size agnostic** - Uses memory-mapped lazy-trie for all key-value pairs
- No key-value separation mechanism mentioned
- Lazy-trie provides uniform access patterns regardless of value size
- Memory-mapping allows OS to handle paging of large values
- WAL-based persistence for all operations
- Best for workloads with predictable access patterns and moderate value sizes

---


## BVLSM

### Summary
BVLSM (2024) implements WAL-time KV separation with significant performance improvements. Achieves 7.6x throughput over RocksDB and 1.9x over BlobDB. Very recent, highly relevant to TideHunter's WAL approach.

### Papers & Publications
- **Paper**: ["BVLSM: Write-Efficient LSM-Tree Storage via WAL-Time Key-Value Separation"](https://arxiv.org/abs/2506.04678) (arXiv 2024)
- **Authors**: Wendi Cheng
- **Code**: Not publicly available

### Scores
1. **Recency & Maintenance**: 3 (2024)
2. **Academic Standing**: 2 (Recent arXiv)
3. **Technical Relevance**: 3 (WAL-time separation)
4. **Core Innovation**: 3 (WAL-level KV separation)
5. **Code Quality**: 1 (Unknown)
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 0 (No implementation available)

**Total Score: 14/21**

### Recommendation
**Highly relevant concept** - Most similar to TideHunter's WAL focus, but code availability uncertain.

### Value Size Optimization
- **Specifically designed for large values** - WAL-time separation for "big-value items"
- Uses predefined size threshold for value separation (tested with 64KB)
- Near-linear performance growth as value size increases, plateaus at 16KB
- Achieves 7.6x throughput over RocksDB and 1.9x over BlobDB with 64KB values
- Big values redirected to separate BValue file, only metadata in MemTable
- Reduces memory pressure and WAL write volume for large values
- Optimized for multimedia objects and ML embeddings

---

## redb

### Summary
Redb is a Rust embedded KV database inspired by LMDB, developed by Christopher Berner (cberner). Reached 1.0 stable release on June 16, 2023. Relevant as another Rust implementation for comparison.

### Technical Details
- **Language**: Rust (like TideHunter)
- **Code**: https://github.com/cberner/redb
- **Architecture**: ACID, MVCC, inspired by LMDB

### Scores
1. **Recency & Maintenance**: 3 (Active)
2. **Academic Standing**: 1 (No paper)
3. **Technical Relevance**: 2 (Different architecture)
4. **Core Innovation**: 1 (LMDB-inspired)
5. **Code Quality**: 3 (Rust, active)
6. **Benchmark Coverage**: 2
7. **Ease of Comparison**: 3 (Pure Rust implementation!)

**Total Score: 15/21**

### Recommendation
**Skip** - Useful for Rust comparison but architecturally less interesting.

### Value Size Optimization
- **Better than LMDB for large values** - Supports up to 3.75GiB key-value pairs
- Copy-on-write B-trees with fixed 4KB pages
- No key-value separation mechanism
- More robust than LMDB for large values (LMDB can lose data)
- No specific optimization for small vs large values
- Good for workloads with moderate to large values that fit in memory

---

## Summary Table

| System | Score | Year | Paper | Code | Key Strength | Rust Compatibility |
|--------|-------|------|-------|------|--------------|-------------------|
| **LMDB** | 20/21 | Active | No | Yes | Memory-mapped, no WAL | Excellent (lmdb-rs) |
| **RocksDB** | 19/21 | Active | Yes | Yes | Industry standard | Excellent (rust-rocksdb) |
| **FASTER** | 19/21 | 2018+ | Yes | Yes | Concurrency, 160M ops/sec | Moderate (faster-rs experimental) |
| **Fjall** | 18/21 | 2025 (v2.8) | No | Yes | Pure Rust KV separation | Perfect (Pure Rust!) |
| **BlobDB** | 18/21 | 2021 | No | Yes | Large values in RocksDB | Excellent (via rust-rocksdb) |
| **DiffKV** | 17/21 | 2021 | Yes | Yes | Latest KV separation | Poor (C++, no bindings) |
| **ADOC** | 17/21 | 2023 | Yes | Yes | 87.9% write stall reduction | Moderate (modified RocksDB) |
| **SpanDB** | 17/21 | 2021 | Yes | Yes | WAL on fast storage | Poor (C++ with SPDK) |
| **Titan** | 16/21 | Active | No | Yes | Production KV separation | Moderate (RocksDB plugin) |
| **TerarkDB** | 16/21 | 2021 | No | Yes | ByteDance optimized LSM | Excellent (RocksDB-compatible) |
| **Bourbon** | 15/21 | 2020 | Yes | Yes | Learned indexes + KV sep | Poor (C++, no bindings) |
| **PebblesDB** | 15/21 | 2017 | Yes | Yes | 6.7x write throughput | Poor (C++, no bindings) |
| **SplinterDB** | 15/21 | 2020/2022 | Yes | Yes | 6-10x faster insertions | Poor (C, no bindings) |
| **redb** | 15/21 | 2023 (v1.0) | No | Yes | Rust implementation | Perfect (Pure Rust!) |
| **KVell** | 15/21 | 2019 | Yes | Yes | NVMe optimization | Poor (C, no bindings) |
| **HashKV** | 15/21 | 2018 | Yes | Yes | 4.6x update throughput | Poor (C++, no bindings) |
| **BVLSM** | 14/21 | 2024 | Yes | ? | 7.6x throughput vs RocksDB | None (no implementation?) |
| **Monkey** | 13/21 | 2017 | Yes | Yes | 50-90% lookup cost reduction | Poor (C++/Java) |
| **Dostoevsky** | 13/21 | 2018 | Yes | Yes | Lazy leveling, Fluid LSM | Poor (C++, no bindings) |
| **AgateDB** | 12/21 | Active | No | Yes | Pure Rust KV store | Perfect (Pure Rust!) |
| **CedrusDB** | 12/21 | 2020 | Yes | ? | WAL + memory-mapped | None (unknown impl) |

## Top Recommendations

### Essential Baselines (Must Have)
1. **RocksDB** - Industry standard, everyone knows it
2. **RocksDB + BlobDB** - For large value comparisons

### Choose ONE Additional System Based on Focus:

#### For Architectural Diversity
**LMDB** (Score: 20/21)
- Pros: Completely different architecture (B-tree, memory-mapped, no WAL)
- Cons: No key-value separation for large values
- Why: Best contrast to show TideHunter's hybrid WAL + memory-mapped approach

#### For Large Value Performance
**DiffKV** (Score: 17/21)
- Pros: Latest academic KV separation, extensive benchmarks, available code
- Cons: Still LSM-based
- Why: State-of-the-art academic baseline for large values

#### For Production Validation
**Titan** (Score: 17/21)
- Pros: Production-tested KV separation in TiDB
- Cons: No academic paper
- Why: Shows TideHunter vs real-world deployed system

#### For Concurrency/Throughput
**FASTER** (Score: 19/21)
- Pros: 160M ops/sec, Microsoft quality, different concurrency model
- Cons: Complex architecture might overshadow comparisons
- Why: Shows maximum throughput achievable

## Final Recommendation

**For an academic paper comparing TideHunter (considering ease of comparison):**

Use these three baselines:
1. **RocksDB** (without BlobDB) - Standard LSM baseline
   - Score: 19/21
   - **Excellent rust-rocksdb bindings**

2. **RocksDB with BlobDB** - Large value optimized LSM
   - Score: 18/21
   - **Same rust-rocksdb bindings**

3. **LMDB** - Memory-mapped without WAL
   - Score: 20/21
   - **Excellent lmdb-rs bindings**

4. **Fjall** - Modern Rust LSM with key-value separation
   - Score: 18/21
   - **Pure Rust** - zero FFI overhead
   - Active development (v2.8 March 2025)
   - Built-in large value optimization

**Why this combination is ideal:**
- All four have **excellent Rust bindings** (ease score ≥ 3)
- Can use same benchmark harness across all systems
- Minimal FFI/wrapper complexity (zero for Fjall)
- Industry standards with architectural diversity
- Clear comparison story: LSM vs memory-mapped vs pure Rust vs TideHunter's hybrid

**Alternative options:**

1. Consider **TerarkDB** (Score: 16/21) for production comparison:
   - ByteDance's production-optimized RocksDB fork
   - RocksDB-compatible API for easy integration
   - Real-world deployment at scale

2. Consider **redb** (Score: 15/21) as alternative Rust baseline:
   - **Pure Rust** - perfect compatibility
   - LMDB-inspired but Rust-native
   - Direct performance comparison without FFI overhead

2. For WAL-specific comparisons, consider **SpanDB** (Score: 17/21):
   - WAL optimization on fast NVMe - directly relevant to TideHunter
   - 8.8× throughput improvement over RocksDB
   - Requires SPDK setup but provides strong WAL baseline

**NEW systems worth considering (added in this evaluation):**
- **SpanDB** (17/21) - WAL optimization on fast storage, highly relevant but requires SPDK
- **ADOC** (17/21) - State-of-the-art write stall reduction, could use modified RocksDB

**NOT recommended despite high relevance:**
- **DiffKV** (17/21) - C++ with no Rust bindings
- **SpanDB** (17/21) - Requires SPDK setup, no Rust bindings
- **ADOC** (17/21) - Research quality, complex integration
- **Titan** (16/21) - Requires complex RocksDB plugin setup
- **FASTER** (19/21) - Experimental Rust wrapper only

## Implementation Notes for Benchmarking

### Setting up comparisons with Rust:
1. **RocksDB/BlobDB**: Use `rust-rocksdb` crate
   ```toml
   [dependencies]
   rocksdb = "0.21"
   ```

2. **LMDB**: Use `lmdb-rs` crate
   ```toml
   [dependencies]
   lmdb = "0.11"
   ```

3. **Fjall**: Native Rust
   ```toml
   [dependencies]
   fjall = "2.8"
   ```

4. **redb**: Native Rust
   ```toml
   [dependencies]
   redb = "1.5"
   ```

5. **Common benchmark harness**: Can use same Rust benchmark framework (criterion, YCSB-rs, or custom) across all systems

This allows direct, fair comparisons with minimal language overhead.

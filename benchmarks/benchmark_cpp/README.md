# C++ Benchmark for Key-Value Storage Backends

This is a C++ implementation of a benchmarking framework that matches the behavior of the Rust benchmark. It supports testing various C++ storage backends including RocksDB, LMDB, DiffKV, FASTER, Titan, and others.

## Features

- Identical behavior to the Rust benchmark implementation
- Multi-threaded write and mixed read/write workloads
- Support for multiple key distribution patterns (Uniform, Zipf, SequenceChoice, ChoiceSequence)
- Configurable read modes (GET, LT, EXISTS)
- Background writer threads
- Detailed latency histograms and throughput metrics

## Prerequisites

### Required
- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2017+)
- CMake 3.14 or higher
- POSIX threads support

### Optional
- Boost 1.65+ (for advanced CLI options)
- RocksDB development libraries
- LMDB development libraries
- Other backend libraries as needed

## Building

### Basic Build

```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
```

### Build with Specific Backends

```bash
cmake .. -DENABLE_ROCKSDB=ON -DENABLE_LMDB=ON
make -j$(nproc)
```

### Build with All Features

```bash
# Install dependencies (Ubuntu/Debian)
sudo apt-get install librocksdb-dev liblmdb-dev libboost-all-dev

# Install dependencies (macOS)
brew install rocksdb lmdb boost

# Build
mkdir build
cd build
cmake .. -DENABLE_ROCKSDB=ON -DENABLE_LMDB=ON
make -j$(nproc)
```

## Usage

### Basic Usage

```bash
# Run with default settings (RocksDB backend)
./benchmark_cpp

# Specify backend
./benchmark_cpp --backend rocksdb

# Configure workload
./benchmark_cpp --writes 1000000 --write-threads 4 --operations 2000000
```

### Command Line Options

- `--backend <name>`: Storage backend (rocksdb, blobdb, lmdb, faster, diffkv, titan)
- `--write-threads <n>`: Number of write threads (default: 1)
- `--mixed-threads <n>`: Number of mixed read/write threads (default: 1)
- `--writes <n>`: Number of writes per thread (default: 1000000)
- `--operations <n>`: Operations in mixed phase (default: 1000000)
- `--write-size <bytes>`: Value size in bytes (default: 1024)
- `--key-len <bytes>`: Key length in bytes (default: 32)
- `--read-percentage <0-100>`: Percentage of reads in mixed phase (default: 100)
- `--background-writes <n>`: Background writes per second (default: 0)
- `--key-layout <type>`: Key distribution pattern (u, sc, cs, zipf)
- `--read-mode <mode>`: Read operation mode (get, lt, exists)
- `--path <dir>`: Storage directory path
- `--preserve`: Keep database after benchmark
- `--help`: Show help message

### Examples

```bash
# Heavy write workload
./benchmark_cpp --backend rocksdb --writes 10000000 --write-threads 8

# Mixed read/write with Zipf distribution
./benchmark_cpp --backend lmdb --operations 5000000 --read-percentage 80 --key-layout zipf

# Range queries with background writes
./benchmark_cpp --backend rocksdb --read-mode lt --background-writes 1000

# Using BlobDB (RocksDB with key-value separation)
./benchmark_cpp --backend blobdb --write-size 4096
```

## Output Format

The benchmark outputs results in a format matching the Rust implementation:

```
DB parameters: {
  backend: rocksdb
}
Stress client parameters: {
  write_threads: 1
  mixed_threads: 1
  writes: 1000000
  operations: 1000000
  write_size: 1024
  key_len: 32
  read_percentage: 100
  background_writes: 0
  key_layout: Uniform
  read_mode: Get
  preserve: false
}

=== Write Phase ===
Progress: 100.0% (1,000,000/1,000,000)
Write phase completed in 2m 30s
Throughput: 6.67 Kops/s
Bandwidth: 6.67 MB/s

=== Mixed Phase ===
Read percentage: 100%
Mixed phase completed in 1m 45s
Throughput: 9.52 Kops/s

=== Final Summary ===
Duration: 4m 15s
Write Operations:
  Total: 1,000,000
  Throughput: 3.92 Kops/s
  Bandwidth: 4.01 MB/s
Write Latency Statistics:
  Count: 1,000,000
  Mean: 254.32 µs
  Min: 100 µs
  P50: 200 µs
  P90: 400 µs
  P99: 800 µs
  P99.9: 2 ms
  Max: 10 ms
```

## Adding New Backends

To add support for a new C++ backend:

1. Create header and implementation files in `include/storage/` and `src/storage/`
2. Inherit from the `Storage` interface
3. Implement required methods: `insert()`, `get()`, `get_lt()`, `exists()`, `name()`
4. Add backend to `Backend` enum in `config.h`
5. Update `storage_factory.cpp` to instantiate your backend
6. Add CMake option in `CMakeLists.txt`

Example:
```cpp
// include/storage/mydb_storage.h
class MyDBStorage : public Storage {
public:
    explicit MyDBStorage(const std::string& path);
    void insert(const std::string& key, const std::string& value) override;
    std::optional<std::string> get(const std::string& key) override;
    // ... other methods
};
```

## Troubleshooting

### Missing Backends
If a backend is not available, ensure:
- Development libraries are installed
- CMake can find the libraries (check CMake output)
- Backend is enabled with `-DENABLE_<BACKEND>=ON`

### Performance Issues
- Ensure release build: `cmake .. -DCMAKE_BUILD_TYPE=Release`
- Check filesystem: Use fast SSD/NVMe storage
- Verify no other processes are competing for I/O

### Compilation Errors
- Ensure C++17 support
- Check all required headers are installed
- Verify Boost version if using advanced CLI options

## License

This benchmark framework follows the same license as the parent project.
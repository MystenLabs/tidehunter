cmake_minimum_required(VERSION 3.14)
project(benchmark_cpp VERSION 1.0.0)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Options for enabling different backends
option(ENABLE_ROCKSDB "Enable RocksDB backend" ON)
option(ENABLE_LMDB "Enable LMDB backend" OFF)
option(ENABLE_FASTER "Enable FASTER backend" OFF)
option(ENABLE_DIFFKV "Enable DiffKV backend" OFF)
option(ENABLE_TITAN "Enable Titan backend" OFF)

# Find required packages
find_package(Threads REQUIRED)

# Find optional packages
find_package(Boost 1.65 COMPONENTS program_options filesystem system)
if(NOT Boost_FOUND)
    message(STATUS "Boost not found, will build without advanced CLI options")
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Source files
set(BENCHMARK_SOURCES
    src/main.cpp
    src/benchmark/stress.cpp
    src/benchmark/metrics.cpp
    src/benchmark/config.cpp
    src/utils/random.cpp
    src/utils/format.cpp
    src/storage/storage_factory.cpp
)

# Create the benchmark executable
add_executable(benchmark_cpp ${BENCHMARK_SOURCES})

# Link with required libraries
target_link_libraries(benchmark_cpp ${CMAKE_THREAD_LIBS_INIT})

# Link with Boost if available
if(Boost_FOUND)
    target_compile_definitions(benchmark_cpp PRIVATE HAS_BOOST)
    target_link_libraries(benchmark_cpp
        Boost::program_options
        Boost::filesystem
        Boost::system
    )
endif()

# Backend-specific configuration
if(ENABLE_ROCKSDB)
    # Check if RocksDB is available as a submodule
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/rocksdb/CMakeLists.txt")
        add_subdirectory(third_party/rocksdb)
        set(ROCKSDB_LIBRARIES rocksdb)
    else()
        # Try to find system RocksDB
        find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h)
        find_library(ROCKSDB_LIBRARIES rocksdb)

        if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARIES)
            message(WARNING "RocksDB not found. Please install RocksDB or add as submodule.")
            set(ENABLE_ROCKSDB OFF)
        else()
            include_directories(${ROCKSDB_INCLUDE_DIR})
        endif()
    endif()

    if(ENABLE_ROCKSDB)
        target_sources(benchmark_cpp PRIVATE src/storage/rocksdb_storage.cpp)
        target_link_libraries(benchmark_cpp ${ROCKSDB_LIBRARIES})
        target_compile_definitions(benchmark_cpp PRIVATE HAS_ROCKSDB)

        # Link additional libraries required by RocksDB
        find_library(ZLIB z)
        find_library(BZ2_LIB bz2)
        find_library(LZ4_LIB lz4)
        find_library(SNAPPY_LIB snappy)
        find_library(ZSTD_LIB zstd)

        if(ZLIB)
            target_link_libraries(benchmark_cpp ${ZLIB})
        endif()
        if(BZ2_LIB)
            target_link_libraries(benchmark_cpp ${BZ2_LIB})
        endif()
        if(LZ4_LIB)
            target_link_libraries(benchmark_cpp ${LZ4_LIB})
        endif()
        if(SNAPPY_LIB)
            target_link_libraries(benchmark_cpp ${SNAPPY_LIB})
        endif()
        if(ZSTD_LIB)
            target_link_libraries(benchmark_cpp ${ZSTD_LIB})
        endif()
    endif()
endif()

if(ENABLE_LMDB)
    find_path(LMDB_INCLUDE_DIR lmdb.h)
    find_library(LMDB_LIBRARIES lmdb)

    if(LMDB_INCLUDE_DIR AND LMDB_LIBRARIES)
        include_directories(${LMDB_INCLUDE_DIR})
        target_sources(benchmark_cpp PRIVATE src/storage/lmdb_storage.cpp)
        target_link_libraries(benchmark_cpp ${LMDB_LIBRARIES})
        target_compile_definitions(benchmark_cpp PRIVATE HAS_LMDB)
    else()
        message(WARNING "LMDB not found. Please install LMDB.")
        set(ENABLE_LMDB OFF)
    endif()
endif()

# Set optimization flags for Release build
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(benchmark_cpp PRIVATE -O3 -march=native)
    elseif(MSVC)
        target_compile_options(benchmark_cpp PRIVATE /O2)
    endif()
endif()

# Set warning flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(benchmark_cpp PRIVATE -Wall -Wextra -Wpedantic)
elseif(MSVC)
    target_compile_options(benchmark_cpp PRIVATE /W4)
endif()

# Installation rules
install(TARGETS benchmark_cpp DESTINATION bin)
install(DIRECTORY config/ DESTINATION etc/benchmark_cpp)

# Print configuration summary
message(STATUS "")
message(STATUS "Configuration Summary:")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Boost Found: ${Boost_FOUND}")
message(STATUS "  RocksDB: ${ENABLE_ROCKSDB}")
message(STATUS "  LMDB: ${ENABLE_LMDB}")
message(STATUS "  FASTER: ${ENABLE_FASTER}")
message(STATUS "  DiffKV: ${ENABLE_DIFFKV}")
message(STATUS "  Titan: ${ENABLE_TITAN}")
message(STATUS "")
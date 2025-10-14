// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// C interface for FASTER and F2 key-value stores
// Adapted from faster-rs and extended for F2 support

#ifndef FASTER_C_H_
#define FASTER_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// Opaque handle types
typedef struct faster_t faster_t;
typedef struct f2_t f2_t;

// Status codes
typedef enum {
    FASTER_SUCCESS = 0,
    FASTER_PENDING = 1,
    FASTER_NOT_FOUND = 2,
    FASTER_OUT_OF_MEMORY = 3,
    FASTER_IO_ERROR = 4,
    FASTER_CORRUPTED = 5,
    FASTER_ABORTED = 6,
    FASTER_ERROR = 7
} faster_status;

// FASTER Configuration
typedef struct {
    const char* storage_path;      // Directory for storage files
    uint64_t initial_log_size;     // Initial hybrid log size in bytes
    uint64_t max_log_size;         // Maximum log size in bytes
    uint64_t page_size;            // Page size (default 2MB)
    uint64_t segment_size;         // File segment size in bytes
    uint64_t hash_table_size;      // Number of hash buckets
    bool enable_read_cache;        // Enable read caching
    uint64_t read_cache_size;      // Read cache size in bytes
    double log_mutable_fraction;   // Fraction of log that is mutable (default 0.9)
} faster_config_t;

// F2 Configuration
typedef struct {
    const char* storage_path;      // Directory for storage files
    uint64_t hot_store_size;       // Hot tier size in bytes
    uint64_t cold_store_size;      // Cold tier size in bytes
    uint64_t index_size;           // Number of hash buckets
    uint64_t read_cache_size;      // Read cache size in bytes
    double hot_threshold;          // When to move to cold (0.0-1.0, e.g., 0.8 = 80% full)
    double cold_threshold;         // When to promote to hot (0.0-1.0, e.g., 0.1 = 10% access rate)
    uint64_t segment_size;         // File segment size in bytes
} f2_config_t;

// Checkpoint and recovery results
typedef struct {
    bool checked;
    char* token;
} faster_checkpoint_result;

typedef struct {
    uint8_t status;
    uint32_t version;
    int session_ids_count;
    char* session_ids;
} faster_recover_result;

// ============ FASTER API ============

// Store lifecycle
faster_t* faster_create(const faster_config_t* config);
void faster_destroy(faster_t* store);

// Session management
const char* faster_start_session(faster_t* store);
uint64_t faster_continue_session(faster_t* store, const char* token);
void faster_stop_session(faster_t* store);
void faster_refresh_session(faster_t* store);
void faster_complete_pending(faster_t* store, bool wait);

// Basic operations (no RMW as requested)
faster_status faster_upsert(faster_t* store,
                            const void* key, uint64_t key_length,
                            const void* value, uint64_t value_length,
                            uint64_t monotonic_serial_number);

faster_status faster_read(faster_t* store,
                          const void* key, uint64_t key_length,
                          void** value, uint64_t* value_length,
                          uint64_t monotonic_serial_number);

faster_status faster_delete(faster_t* store,
                           const void* key, uint64_t key_length,
                           uint64_t monotonic_serial_number);

// Checkpoint/Recovery
faster_checkpoint_result* faster_checkpoint(faster_t* store);
faster_checkpoint_result* faster_checkpoint_index(faster_t* store);
faster_checkpoint_result* faster_checkpoint_hybrid_log(faster_t* store);
faster_recover_result* faster_recover(faster_t* store,
                                      const char* index_token,
                                      const char* hybrid_log_token);

// Statistics and maintenance
uint64_t faster_size(faster_t* store);
bool faster_grow_index(faster_t* store);
void faster_dump_distribution(faster_t* store);

// Memory management
void faster_free_value(void* value);
void faster_free_checkpoint_result(faster_checkpoint_result* result);
void faster_free_recover_result(faster_recover_result* result);

// ============ F2 API (NEW) ============

// Store lifecycle
f2_t* f2_create(const f2_config_t* config);
void f2_destroy(f2_t* store);

// Session management
const char* f2_start_session(f2_t* store);
uint64_t f2_continue_session(f2_t* store, const char* token);
void f2_stop_session(f2_t* store);
void f2_refresh_session(f2_t* store);
void f2_complete_pending(f2_t* store, bool wait);

// Basic operations
faster_status f2_upsert(f2_t* store,
                        const void* key, uint64_t key_length,
                        const void* value, uint64_t value_length,
                        uint64_t monotonic_serial_number);

faster_status f2_read(f2_t* store,
                      const void* key, uint64_t key_length,
                      void** value, uint64_t* value_length,
                      uint64_t monotonic_serial_number);

faster_status f2_delete(f2_t* store,
                        const void* key, uint64_t key_length,
                        uint64_t monotonic_serial_number);

// Checkpoint/Recovery
faster_checkpoint_result* f2_checkpoint(f2_t* store);
faster_recover_result* f2_recover(f2_t* store, const char* checkpoint_token);

// Statistics
uint64_t f2_size(f2_t* store);
uint64_t f2_hot_size(f2_t* store);
uint64_t f2_cold_size(f2_t* store);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  /* FASTER_C_H_ */
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle types
typedef struct faster_fixed_t faster_fixed_t;
typedef struct f2_fixed_t f2_fixed_t;

// Status codes
typedef enum {
  FASTER_OK = 0,
  FASTER_PENDING = 1,
  FASTER_NOT_FOUND = 2,
  FASTER_OUT_OF_MEMORY = 3,
  FASTER_IO_ERROR = 4,
  FASTER_CORRUPTION = 5,
  FASTER_ABORTED = 6,
} faster_status_t;

// ============ FASTER Fixed-Size API ============

// Create a new FASTER store with fixed-size keys and values
// table_size: Number of hash table buckets
// log_size: Size of the in-memory log in bytes
// storage_path: Directory path for persistent storage
faster_fixed_t* faster_fixed_create(
    uint64_t table_size,
    uint64_t log_size,
    const char* storage_path);

// Destroy a FASTER store
void faster_fixed_destroy(faster_fixed_t* store);

// Insert or update a key-value pair
// key: Pointer to key data (must be faster_fixed_key_size() bytes)
// value: Pointer to value data (must be faster_fixed_value_size() bytes)
faster_status_t faster_fixed_upsert(
    faster_fixed_t* store,
    const void* key,
    const void* value);

// Read a value by key
// key: Pointer to key data (must be faster_fixed_key_size() bytes)
// value: Output buffer for value (must be faster_fixed_value_size() bytes)
faster_status_t faster_fixed_read(
    faster_fixed_t* store,
    const void* key,
    void* value);

// Read-modify-write operation
// key: Pointer to key data
// callback: Function to apply the modification
// user_data: User data passed to callback
faster_status_t faster_fixed_rmw(
    faster_fixed_t* store,
    const void* key,
    void (*callback)(const void* old_value, void* new_value, void* user_data),
    void* user_data);

// Delete a key
faster_status_t faster_fixed_delete(
    faster_fixed_t* store,
    const void* key);

// Refresh the session
void faster_fixed_refresh(faster_fixed_t* store);

// Complete pending operations
void faster_fixed_complete_pending(faster_fixed_t* store, bool wait);

// ============ F2 Fixed-Size API ============

// Create a new F2 store with fixed-size keys and values
// hot_table_size: Number of hash table buckets for hot store
// hot_log_size: Size of the in-memory log for hot store
// cold_table_size: Number of hash table buckets for cold store
// cold_log_size: Size of the in-memory log for cold store
// storage_path: Directory path for persistent storage
f2_fixed_t* f2_fixed_create(
    uint64_t hot_table_size,
    uint64_t hot_log_size,
    uint64_t cold_table_size,
    uint64_t cold_log_size,
    const char* storage_path);

// Destroy an F2 store
void f2_fixed_destroy(f2_fixed_t* store);

// Insert or update a key-value pair
faster_status_t f2_fixed_upsert(
    f2_fixed_t* store,
    const void* key,
    const void* value);

// Read a value by key
faster_status_t f2_fixed_read(
    f2_fixed_t* store,
    const void* key,
    void* value);

// Refresh the session
void f2_fixed_refresh(f2_fixed_t* store);

// Complete pending operations
void f2_fixed_complete_pending(f2_fixed_t* store, bool wait);

// ============ Utility Functions ============

// Get the fixed key size (in bytes)
size_t faster_fixed_key_size(void);

// Get the fixed value size (in bytes)
size_t faster_fixed_value_size(void);

#ifdef __cplusplus
}
#endif
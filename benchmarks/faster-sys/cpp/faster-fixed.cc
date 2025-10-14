// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// C API wrapper for fixed-size FASTER and F2

#include "faster-fixed.h"
#include <cstdlib>
#include <filesystem>

// Use 8-byte keys and 100-byte values by default
// These can be changed at compile time
constexpr size_t KEY_SIZE = 8;
constexpr size_t VALUE_SIZE = 100;

using FasterStore = FixedFasterKv<KEY_SIZE, VALUE_SIZE>;
using F2Store = FixedF2Kv<KEY_SIZE, VALUE_SIZE>;

// ============ C API Structures ============

extern "C" {

// Opaque handle types
typedef struct faster_fixed_t {
  FasterStore* store;
} faster_fixed_t;

typedef struct f2_fixed_t {
  F2Store* store;
} f2_fixed_t;

// Status codes matching FASTER's Status enum
typedef enum {
  FASTER_OK = 0,
  FASTER_PENDING = 1,
  FASTER_NOT_FOUND = 2,
  FASTER_OUT_OF_MEMORY = 3,
  FASTER_IO_ERROR = 4,
  FASTER_CORRUPTION = 5,
  FASTER_ABORTED = 6,
} faster_status_t;

// Convert internal Status to C API status
static faster_status_t convert_status(Status status) {
  switch(status) {
    case Status::Ok: return FASTER_OK;
    case Status::Pending: return FASTER_PENDING;
    case Status::NotFound: return FASTER_NOT_FOUND;
    case Status::OutOfMemory: return FASTER_OUT_OF_MEMORY;
    case Status::IOError: return FASTER_IO_ERROR;
    case Status::Corruption: return FASTER_CORRUPTION;
    case Status::Aborted: return FASTER_ABORTED;
    default: return FASTER_CORRUPTION;
  }
}

// ============ FASTER Fixed API ============

faster_fixed_t* faster_fixed_create(
    uint64_t table_size,
    uint64_t log_size,
    const char* storage_path) {
  try {
    std::filesystem::create_directories(storage_path);

    auto* wrapper = new faster_fixed_t;
    wrapper->store = new FasterStore(table_size, log_size, storage_path);
    return wrapper;
  } catch (...) {
    return nullptr;
  }
}

void faster_fixed_destroy(faster_fixed_t* store) {
  if (store) {
    delete store->store;
    delete store;
  }
}

faster_status_t faster_fixed_upsert(
    faster_fixed_t* store,
    const void* key,
    const void* value) {
  if (!store || !key || !value) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Upsert(key, value));
}

faster_status_t faster_fixed_read(
    faster_fixed_t* store,
    const void* key,
    void* value) {
  if (!store || !key || !value) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Read(key, value));
}

faster_status_t faster_fixed_rmw(
    faster_fixed_t* store,
    const void* key,
    void (*callback)(const void* old_value, void* new_value, void* user_data),
    void* user_data) {
  if (!store || !key) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Rmw(key, callback, user_data));
}

faster_status_t faster_fixed_delete(
    faster_fixed_t* store,
    const void* key) {
  if (!store || !key) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Delete(key));
}

void faster_fixed_refresh(faster_fixed_t* store) {
  if (store) {
    store->store->Refresh();
  }
}

void faster_fixed_complete_pending(faster_fixed_t* store, bool wait) {
  if (store) {
    store->store->CompletePending(wait);
  }
}

// ============ F2 Fixed API ============

f2_fixed_t* f2_fixed_create(
    uint64_t hot_table_size,
    uint64_t hot_log_size,
    uint64_t cold_table_size,
    uint64_t cold_log_size,
    const char* storage_path) {
  try {
    std::filesystem::create_directories(storage_path);

    // Create F2 configuration
    F2Config config;

    // Configure hot store
    config.hot_config.index_config.num_buckets = hot_table_size;
    config.hot_config.hlog_config.in_mem_size = hot_log_size;
    config.hot_config.hlog_config.mutable_fraction = 0.9;
    config.hot_config.hlog_config.pre_allocate = false;

    // Configure cold store
    config.cold_config.index_config.num_buckets = cold_table_size;
    config.cold_config.hlog_config.in_mem_size = cold_log_size;
    config.cold_config.hlog_config.mutable_fraction = 0.1;  // Less mutable for cold
    config.cold_config.hlog_config.pre_allocate = false;

    // Configure promotion/demotion thresholds
    config.promotion_threshold_freq = 2;  // Promote after 2 accesses
    config.demotion_threshold_ms = 60000; // Demote after 60 seconds

    auto* wrapper = new f2_fixed_t;
    wrapper->store = new F2Store(config, storage_path);
    return wrapper;
  } catch (...) {
    return nullptr;
  }
}

void f2_fixed_destroy(f2_fixed_t* store) {
  if (store) {
    delete store->store;
    delete store;
  }
}

faster_status_t f2_fixed_upsert(
    f2_fixed_t* store,
    const void* key,
    const void* value) {
  if (!store || !key || !value) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Upsert(key, value));
}

faster_status_t f2_fixed_read(
    f2_fixed_t* store,
    const void* key,
    void* value) {
  if (!store || !key || !value) {
    return FASTER_CORRUPTION;
  }
  return convert_status(store->store->Read(key, value));
}

void f2_fixed_refresh(f2_fixed_t* store) {
  if (store) {
    store->store->Refresh();
  }
}

void f2_fixed_complete_pending(f2_fixed_t* store, bool wait) {
  if (store) {
    store->store->CompletePending(wait);
  }
}

// ============ Utility Functions ============

size_t faster_fixed_key_size() {
  return KEY_SIZE;
}

size_t faster_fixed_value_size() {
  return VALUE_SIZE;
}

} // extern "C"
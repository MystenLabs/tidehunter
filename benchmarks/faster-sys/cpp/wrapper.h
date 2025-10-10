#ifndef FASTER_WRAPPER_H
#define FASTER_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// Opaque handle types
typedef struct FasterKv FasterKv;
typedef struct F2Kv F2Kv;

// Status codes
typedef enum {
    FASTER_SUCCESS = 0,
    FASTER_ERROR = 1,
    FASTER_NOT_FOUND = 2,
    FASTER_PENDING = 3,
} FasterStatus;

// Configuration structures
typedef struct {
    const char* storage_path;
    size_t initial_log_size;
    size_t max_log_size;
    size_t page_size;
    size_t segment_size;
    bool enable_read_cache;
    size_t read_cache_size;
} FasterConfig;

typedef struct {
    const char* storage_path;
    size_t hot_store_size;
    size_t cold_store_size;
    size_t read_cache_size;
    bool enable_tiering;
    double hot_threshold;
    double cold_threshold;
} F2Config;

// FASTER API
FasterKv* faster_create(const FasterConfig* config);
void faster_destroy(FasterKv* store);

FasterStatus faster_insert(FasterKv* store,
                          const void* key, size_t key_len,
                          const void* value, size_t value_len,
                          uint64_t serial);

FasterStatus faster_read(FasterKv* store,
                        const void* key, size_t key_len,
                        void** value, size_t* value_len,
                        uint64_t serial);

FasterStatus faster_upsert(FasterKv* store,
                           const void* key, size_t key_len,
                           const void* value, size_t value_len,
                           uint64_t serial);

FasterStatus faster_delete(FasterKv* store,
                          const void* key, size_t key_len,
                          uint64_t serial);

void faster_complete_pending(FasterKv* store, bool wait);
FasterStatus faster_checkpoint(FasterKv* store, const char* token);
FasterStatus faster_recover(FasterKv* store, const char* token);

// F2 API
F2Kv* f2_create(const F2Config* config);
void f2_destroy(F2Kv* store);

FasterStatus f2_insert(F2Kv* store,
                       const void* key, size_t key_len,
                       const void* value, size_t value_len,
                       uint64_t serial);

FasterStatus f2_read(F2Kv* store,
                     const void* key, size_t key_len,
                     void** value, size_t* value_len,
                     uint64_t serial);

FasterStatus f2_upsert(F2Kv* store,
                       const void* key, size_t key_len,
                       const void* value, size_t value_len,
                       uint64_t serial);

FasterStatus f2_delete(F2Kv* store,
                       const void* key, size_t key_len,
                       uint64_t serial);

void f2_complete_pending(F2Kv* store, bool wait);
FasterStatus f2_checkpoint(F2Kv* store, const char* token);
FasterStatus f2_recover(F2Kv* store, const char* token);

// Memory management helpers
void faster_free_value(void* value);

#ifdef __cplusplus
}
#endif

#endif // FASTER_WRAPPER_H
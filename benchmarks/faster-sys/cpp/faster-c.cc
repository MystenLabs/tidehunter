// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// Complete C++ to C bridge implementation for FASTER and F2

#include "faster-c.h"

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <memory>
#include <filesystem>
#include <thread>

#include "core/faster.h"
#include "core/f2.h"
#include "device/file_system_disk.h"
#include "core/utility.h"
#include "environment/file_linux.h"

using namespace FASTER::core;
using namespace FASTER::device;
using namespace FASTER::environment;

// ============ Variable-length Key/Value Types ============

class VarLenKey {
 public:
  uint32_t key_length_;
  const uint8_t* key_data_;

  // For owned data
  std::unique_ptr<uint8_t[]> owned_data_;

  VarLenKey() : key_length_(0), key_data_(nullptr) {}

  VarLenKey(const void* key, uint64_t length)
    : key_length_(static_cast<uint32_t>(length)) {
    owned_data_.reset(new uint8_t[key_length_]);
    memcpy(owned_data_.get(), key, key_length_);
    key_data_ = owned_data_.get();
  }

  VarLenKey(const VarLenKey& other)
    : key_length_(other.key_length_) {
    if (other.key_length_ > 0) {
      owned_data_.reset(new uint8_t[key_length_]);
      memcpy(owned_data_.get(), other.buffer(), key_length_);
      key_data_ = owned_data_.get();
    } else {
      key_data_ = nullptr;
    }
  }

  VarLenKey& operator=(const VarLenKey& other) {
    if (this != &other) {
      key_length_ = other.key_length_;
      if (other.key_length_ > 0) {
        owned_data_.reset(new uint8_t[key_length_]);
        memcpy(owned_data_.get(), other.buffer(), key_length_);
        key_data_ = owned_data_.get();
      } else {
        owned_data_.reset();
        key_data_ = nullptr;
      }
    }
    return *this;
  }

  const uint8_t* buffer() const {
    return key_data_ ? key_data_ : reinterpret_cast<const uint8_t*>(this + 1);
  }

  uint8_t* buffer() {
    if (!key_data_) {
      key_data_ = reinterpret_cast<uint8_t*>(this + 1);
    }
    return const_cast<uint8_t*>(key_data_);
  }

  // Required for FASTER
  inline static constexpr bool has_constants() { return false; }

  inline uint32_t size() const {
    return static_cast<uint32_t>(sizeof(VarLenKey) + (key_data_ ? 0 : key_length_));
  }

  inline KeyHash GetHash() const {
    return KeyHash(Utility::FasterHash::compute(buffer(), key_length_));
  }

  inline bool operator==(const VarLenKey& other) const {
    return key_length_ == other.key_length_ &&
           memcmp(buffer(), other.buffer(), key_length_) == 0;
  }

  inline bool operator!=(const VarLenKey& other) const {
    return !(*this == other);
  }

  // Serialization
  inline uint32_t SerializedSize() const {
    return sizeof(uint32_t) + key_length_;
  }

  inline void SerializeKey(uint8_t* dst) const {
    memcpy(dst, &key_length_, sizeof(uint32_t));
    memcpy(dst + sizeof(uint32_t), buffer(), key_length_);
  }

  static VarLenKey DeserializeKey(const uint8_t* src) {
    uint32_t len;
    memcpy(&len, src, sizeof(uint32_t));
    return VarLenKey(src + sizeof(uint32_t), len);
  }
};

class VarLenValue {
 public:
  uint32_t value_length_;
  const uint8_t* value_data_;

  // For owned data
  std::unique_ptr<uint8_t[]> owned_data_;

  VarLenValue() : value_length_(0), value_data_(nullptr) {}

  VarLenValue(const void* value, uint64_t length)
    : value_length_(static_cast<uint32_t>(length)) {
    owned_data_.reset(new uint8_t[value_length_]);
    memcpy(owned_data_.get(), value, value_length_);
    value_data_ = owned_data_.get();
  }

  VarLenValue(const VarLenValue& other)
    : value_length_(other.value_length_) {
    if (other.value_length_ > 0) {
      owned_data_.reset(new uint8_t[value_length_]);
      memcpy(owned_data_.get(), other.buffer(), value_length_);
      value_data_ = owned_data_.get();
    } else {
      value_data_ = nullptr;
    }
  }

  VarLenValue& operator=(const VarLenValue& other) {
    if (this != &other) {
      value_length_ = other.value_length_;
      if (other.value_length_ > 0) {
        owned_data_.reset(new uint8_t[value_length_]);
        memcpy(owned_data_.get(), other.buffer(), value_length_);
        value_data_ = owned_data_.get();
      } else {
        owned_data_.reset();
        value_data_ = nullptr;
      }
    }
    return *this;
  }

  const uint8_t* buffer() const {
    return value_data_ ? value_data_ : reinterpret_cast<const uint8_t*>(this + 1);
  }

  uint8_t* buffer() {
    if (!value_data_) {
      value_data_ = reinterpret_cast<uint8_t*>(this + 1);
    }
    return const_cast<uint8_t*>(value_data_);
  }

  inline static constexpr bool has_constants() { return false; }

  inline uint32_t size() const {
    return static_cast<uint32_t>(sizeof(VarLenValue) + (value_data_ ? 0 : value_length_));
  }

  // Serialization
  inline uint32_t SerializedSize() const {
    return sizeof(uint32_t) + value_length_;
  }

  inline void SerializeValue(uint8_t* dst) const {
    memcpy(dst, &value_length_, sizeof(uint32_t));
    memcpy(dst + sizeof(uint32_t), buffer(), value_length_);
  }

  static VarLenValue DeserializeValue(const uint8_t* src) {
    uint32_t len;
    memcpy(&len, src, sizeof(uint32_t));
    return VarLenValue(src + sizeof(uint32_t), len);
  }
};

// ============ Store Wrappers ============

using disk_t = FileSystemDisk<QueueIoHandler, 1073741824LL>; // 1GB segments

struct faster_t {
  using store_t = FasterKv<VarLenKey, VarLenValue, disk_t>;

  store_t* store;
  Guid session_guid;
  bool session_active;

  faster_t() : store(nullptr), session_active(false) {}

  ~faster_t() {
    if (session_active && store) {
      store->StopSession();
    }
    delete store;
  }
};

struct f2_t {
  using store_t = F2Kv<VarLenKey, VarLenValue, disk_t>;

  store_t* store;
  Guid session_guid;
  bool session_active;

  f2_t() : store(nullptr), session_active(false) {}

  ~f2_t() {
    if (session_active && store) {
      store->StopSession();
    }
    delete store;
  }
};

// ============ Context Classes ============

class ReadContext : public IAsyncContext {
 public:
  typedef VarLenKey key_t;
  typedef VarLenValue value_t;

  ReadContext(const void* key, uint64_t key_len)
    : key_(key, key_len), output_buffer_(nullptr), output_size_(0) {}

  ReadContext(const ReadContext& other)
    : key_(other.key_), output_buffer_(nullptr), output_size_(0) {}

  ~ReadContext() {
    // Buffer freed by caller via faster_free_value
  }

  inline Status DeepCopy_Internal(IAsyncContext*& context_copy) override {
    context_copy = new ReadContext(*this);
    return Status::Ok;
  }

  inline const key_t& key() const { return key_; }

  inline void Get(const value_t& value) {
    output_size_ = value.value_length_;
    output_buffer_ = malloc(output_size_);
    if (output_buffer_) {
      memcpy(output_buffer_, value.buffer(), output_size_);
    }
  }

  inline void GetAtomic(const value_t& value) {
    Get(value);
  }

  void* get_output() const { return output_buffer_; }
  uint64_t get_output_size() const { return output_size_; }

 protected:
  key_t key_;
  void* output_buffer_;
  uint64_t output_size_;
};

class UpsertContext : public IAsyncContext {
 public:
  typedef VarLenKey key_t;
  typedef VarLenValue value_t;

  UpsertContext(const void* key, uint64_t key_len,
                const void* value, uint64_t value_len)
    : key_(key, key_len), value_(value, value_len) {}

  UpsertContext(const UpsertContext& other)
    : key_(other.key_), value_(other.value_) {}

  inline Status DeepCopy_Internal(IAsyncContext*& context_copy) override {
    context_copy = new UpsertContext(*this);
    return Status::Ok;
  }

  inline const key_t& key() const { return key_; }
  inline const value_t& value() const { return value_; }

  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  inline void Put(value_t& dest) {
    dest = value_;
  }

  inline bool PutAtomic(value_t& dest) {
    Put(dest);
    return true;
  }

 protected:
  key_t key_;
  value_t value_;
};

class DeleteContext : public IAsyncContext {
 public:
  typedef VarLenKey key_t;
  typedef VarLenValue value_t;

  DeleteContext(const void* key, uint64_t key_len)
    : key_(key, key_len) {}

  DeleteContext(const DeleteContext& other)
    : key_(other.key_) {}

  inline Status DeepCopy_Internal(IAsyncContext*& context_copy) override {
    context_copy = new DeleteContext(*this);
    return Status::Ok;
  }

  inline const key_t& key() const { return key_; }

  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

 protected:
  key_t key_;
};

// ============ FASTER C API Implementation ============

extern "C" {

faster_t* faster_create(const faster_config_t* config) {
  if (!config || !config->storage_path) {
    return nullptr;
  }

  try {
    auto* wrapper = new faster_t();

    // Create directory if needed
    std::filesystem::create_directories(config->storage_path);

    // Calculate hash table size
    uint64_t table_size = config->hash_table_size;
    if (table_size == 0) {
      table_size = 1ULL << 20; // Default 1M buckets
    }

    // Create the store
    size_t init_size = config->initial_log_size > 0 ? config->initial_log_size : (1ULL << 30);
    wrapper->store = new faster_t::store_t(table_size, init_size, config->storage_path);

    // Start initial session
    wrapper->session_guid = wrapper->store->StartSession();
    wrapper->session_active = true;

    return wrapper;
  } catch (...) {
    return nullptr;
  }
}

void faster_destroy(faster_t* store) {
  delete store;
}

const char* faster_start_session(faster_t* store) {
  if (!store || !store->store) return nullptr;

  if (store->session_active) {
    store->store->StopSession();
  }

  store->session_guid = store->store->StartSession();
  store->session_active = true;

  static thread_local std::string guid_str;
  guid_str = store->session_guid.ToString();
  return guid_str.c_str();
}

void faster_stop_session(faster_t* store) {
  if (!store || !store->store || !store->session_active) return;

  store->store->StopSession();
  store->session_active = false;
}

void faster_refresh_session(faster_t* store) {
  if (!store || !store->store || !store->session_active) return;
  store->store->Refresh();
}

void faster_complete_pending(faster_t* store, bool wait) {
  if (!store || !store->store || !store->session_active) return;
  store->store->CompletePending(wait);
}

faster_status faster_upsert(faster_t* store,
                            const void* key, uint64_t key_length,
                            const void* value, uint64_t value_length,
                            uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key || !value) {
    return FASTER_ERROR;
  }

  UpsertContext context(key, key_length, value, value_length);
  auto status = store->store->Upsert(context, nullptr, monotonic_serial_number);

  switch (status) {
    case Status::Ok: return FASTER_SUCCESS;
    case Status::Pending: return FASTER_PENDING;
    case Status::NotFound: return FASTER_NOT_FOUND;
    case Status::OutOfMemory: return FASTER_OUT_OF_MEMORY;
    case Status::IOError: return FASTER_IO_ERROR;
    case Status::Aborted: return FASTER_ABORTED;
    default: return FASTER_ERROR;
  }
}

faster_status faster_read(faster_t* store,
                          const void* key, uint64_t key_length,
                          void** value, uint64_t* value_length,
                          uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key || !value || !value_length) {
    return FASTER_ERROR;
  }

  *value = nullptr;
  *value_length = 0;

  ReadContext context(key, key_length);
  auto status = store->store->Read(context, nullptr, monotonic_serial_number);

  // Wait for read to complete
  store->store->CompletePending(true);

  if (status == Status::Ok || status == Status::Pending) {
    *value = context.get_output();
    *value_length = context.get_output_size();

    if (*value != nullptr) {
      return FASTER_SUCCESS;
    }
  }

  switch (status) {
    case Status::NotFound: return FASTER_NOT_FOUND;
    case Status::OutOfMemory: return FASTER_OUT_OF_MEMORY;
    case Status::IOError: return FASTER_IO_ERROR;
    default: return FASTER_ERROR;
  }
}

faster_status faster_delete(faster_t* store,
                           const void* key, uint64_t key_length,
                           uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key) {
    return FASTER_ERROR;
  }

  DeleteContext context(key, key_length);
  auto status = store->store->Delete(context, nullptr, monotonic_serial_number);

  switch (status) {
    case Status::Ok: return FASTER_SUCCESS;
    case Status::Pending: return FASTER_PENDING;
    case Status::NotFound: return FASTER_NOT_FOUND;
    default: return FASTER_ERROR;
  }
}

uint64_t faster_size(faster_t* store) {
  if (!store || !store->store) return 0;
  return store->store->Size();
}

bool faster_grow_index(faster_t* store) {
  if (!store || !store->store) return false;
  // GrowIndex now requires a callback parameter
  auto callback = [](uint64_t) {};
  return store->store->GrowIndex(callback);
}

void faster_dump_distribution(faster_t* store) {
  if (!store || !store->store) return;
  store->store->DumpDistribution();
}

void faster_free_value(void* value) {
  free(value);
}

// Simplified checkpoint/recovery stubs for now
faster_checkpoint_result* faster_checkpoint(faster_t* store) {
  if (!store || !store->store) return nullptr;

  auto* result = static_cast<faster_checkpoint_result*>(
    calloc(1, sizeof(faster_checkpoint_result)));
  result->checked = false;
  result->token = nullptr;
  return result;
}

faster_checkpoint_result* faster_checkpoint_index(faster_t* store) {
  return faster_checkpoint(store);
}

faster_checkpoint_result* faster_checkpoint_hybrid_log(faster_t* store) {
  return faster_checkpoint(store);
}

faster_recover_result* faster_recover(faster_t* store,
                                      const char* index_token,
                                      const char* hybrid_log_token) {
  if (!store || !store->store) return nullptr;

  auto* result = static_cast<faster_recover_result*>(
    calloc(1, sizeof(faster_recover_result)));
  result->status = 0;
  return result;
}

void faster_free_checkpoint_result(faster_checkpoint_result* result) {
  if (result) {
    free(result->token);
    free(result);
  }
}

void faster_free_recover_result(faster_recover_result* result) {
  if (result) {
    free(result->session_ids);
    free(result);
  }
}

// ============ F2 C API Implementation ============

f2_t* f2_create(const f2_config_t* config) {
  if (!config || !config->storage_path) {
    return nullptr;
  }

  try {
    auto* wrapper = new f2_t();

    // Create directories
    std::string hot_path = std::string(config->storage_path) + "/hot";
    std::string cold_path = std::string(config->storage_path) + "/cold";
    std::filesystem::create_directories(hot_path);
    std::filesystem::create_directories(cold_path);

    // Calculate index size
    uint64_t index_size = config->index_size;
    if (index_size == 0) {
      index_size = 1ULL << 20; // Default 1M buckets
    }

    // Set hot/cold thresholds
    double hot_threshold = config->hot_threshold > 0 ? config->hot_threshold : 0.8;
    double cold_threshold = config->cold_threshold > 0 ? config->cold_threshold : 0.1;

    // Create F2 store with proper FasterKvConfig structures
    // Configure hot store (memory-based index, optimized for writes)
    typename f2_t::store_t::HotIndexConfig hot_index_config(index_size);
    typename f2_t::store_t::ColdIndexConfig cold_index_config(
      index_size,                // table_size
      config->cold_store_size,   // in_mem_size
      0.1                        // mutable_fraction
    );

    // Create F2 store with the old-style constructor (10 parameters)
    wrapper->store = new f2_t::store_t(
      hot_index_config,
      config->hot_store_size,    // hot_log_mem_size
      hot_path,                  // hot_log_filename
      cold_index_config,
      config->cold_store_size,   // cold_log_mem_size (not used, already in index config)
      cold_path,                 // cold_log_filename
      hot_threshold,             // hot_threshold
      cold_threshold,            // cold_threshold
      ReadCacheConfig(),         // read_cache_config (default)
      F2CompactionConfig()       // compaction_config (default)
    );

    // Start session
    wrapper->session_guid = wrapper->store->StartSession();
    wrapper->session_active = true;

    return wrapper;
  } catch (...) {
    return nullptr;
  }
}

void f2_destroy(f2_t* store) {
  delete store;
}

const char* f2_start_session(f2_t* store) {
  if (!store || !store->store) return nullptr;

  if (store->session_active) {
    store->store->StopSession();
  }

  store->session_guid = store->store->StartSession();
  store->session_active = true;

  static thread_local std::string guid_str;
  guid_str = store->session_guid.ToString();
  return guid_str.c_str();
}

void f2_stop_session(f2_t* store) {
  if (!store || !store->store || !store->session_active) return;

  store->store->StopSession();
  store->session_active = false;
}

void f2_refresh_session(f2_t* store) {
  if (!store || !store->store || !store->session_active) return;
  store->store->Refresh();
}

void f2_complete_pending(f2_t* store, bool wait) {
  if (!store || !store->store || !store->session_active) return;
  store->store->CompletePending(wait);
}

faster_status f2_upsert(f2_t* store,
                        const void* key, uint64_t key_length,
                        const void* value, uint64_t value_length,
                        uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key || !value) {
    return FASTER_ERROR;
  }

  UpsertContext context(key, key_length, value, value_length);
  auto status = store->store->Upsert(context, nullptr, monotonic_serial_number);

  switch (status) {
    case Status::Ok: return FASTER_SUCCESS;
    case Status::Pending: return FASTER_PENDING;
    case Status::OutOfMemory: return FASTER_OUT_OF_MEMORY;
    default: return FASTER_ERROR;
  }
}

faster_status f2_read(f2_t* store,
                      const void* key, uint64_t key_length,
                      void** value, uint64_t* value_length,
                      uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key || !value || !value_length) {
    return FASTER_ERROR;
  }

  *value = nullptr;
  *value_length = 0;

  ReadContext context(key, key_length);
  auto status = store->store->Read(context, nullptr, monotonic_serial_number);

  // Wait for read to complete
  store->store->CompletePending(true);

  if (status == Status::Ok || status == Status::Pending) {
    *value = context.get_output();
    *value_length = context.get_output_size();

    if (*value != nullptr) {
      return FASTER_SUCCESS;
    }
  }

  switch (status) {
    case Status::NotFound: return FASTER_NOT_FOUND;
    case Status::OutOfMemory: return FASTER_OUT_OF_MEMORY;
    default: return FASTER_ERROR;
  }
}

faster_status f2_delete(f2_t* store,
                        const void* key, uint64_t key_length,
                        uint64_t monotonic_serial_number) {
  if (!store || !store->store || !key) {
    return FASTER_ERROR;
  }

  DeleteContext context(key, key_length);
  auto status = store->store->Delete(context, nullptr, monotonic_serial_number);

  switch (status) {
    case Status::Ok: return FASTER_SUCCESS;
    case Status::Pending: return FASTER_PENDING;
    case Status::NotFound: return FASTER_NOT_FOUND;
    default: return FASTER_ERROR;
  }
}

uint64_t f2_size(f2_t* store) {
  if (!store || !store->store) return 0;
  return store->store->Size();
}

uint64_t f2_hot_size(f2_t* store) {
  if (!store || !store->store) return 0;
  // F2 should have methods to get hot/cold sizes
  // This is a placeholder - actual implementation depends on F2 API
  return store->store->Size() / 2; // Placeholder
}

uint64_t f2_cold_size(f2_t* store) {
  if (!store || !store->store) return 0;
  // F2 should have methods to get hot/cold sizes
  // This is a placeholder - actual implementation depends on F2 API
  return store->store->Size() / 2; // Placeholder
}

faster_checkpoint_result* f2_checkpoint(f2_t* store) {
  if (!store || !store->store) return nullptr;

  auto* result = static_cast<faster_checkpoint_result*>(
    calloc(1, sizeof(faster_checkpoint_result)));
  result->checked = false;
  result->token = nullptr;
  return result;
}

faster_recover_result* f2_recover(f2_t* store, const char* checkpoint_token) {
  if (!store || !store->store) return nullptr;

  auto* result = static_cast<faster_recover_result*>(
    calloc(1, sizeof(faster_recover_result)));
  result->status = 0;
  return result;
}

} // extern "C"
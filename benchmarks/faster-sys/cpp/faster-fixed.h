// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include "core/faster.h"
#include "core/f2.h"
#include "device/file_system_disk.h"
#include "core/utility.h"

using namespace FASTER::core;
using namespace FASTER::device;

// ============ Fixed-size Key/Value Types ============

// Fixed-size key (configurable size at compile time)
template<size_t SIZE>
class FixedKey {
 public:
  uint8_t data[SIZE];

  FixedKey() {
    memset(data, 0, SIZE);
  }

  FixedKey(const void* key_data) {
    memcpy(data, key_data, SIZE);
  }

  FixedKey(const FixedKey& other) {
    memcpy(data, other.data, SIZE);
  }

  FixedKey& operator=(const FixedKey& other) {
    if (this != &other) {
      memcpy(data, other.data, SIZE);
    }
    return *this;
  }

  // Required for FASTER
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(SIZE);
  }

  inline KeyHash GetHash() const {
    return KeyHash(Utility::HashBytes(data, SIZE));
  }

  inline bool operator==(const FixedKey& other) const {
    return memcmp(data, other.data, SIZE) == 0;
  }

  inline bool operator!=(const FixedKey& other) const {
    return !(*this == other);
  }
};

// Fixed-size value (configurable size at compile time)
template<size_t SIZE>
class FixedValue {
 public:
  uint8_t data[SIZE];

  FixedValue() {
    memset(data, 0, SIZE);
  }

  FixedValue(const void* value_data) {
    memcpy(data, value_data, SIZE);
  }

  FixedValue(const FixedValue& other) {
    memcpy(data, other.data, SIZE);
  }

  FixedValue& operator=(const FixedValue& other) {
    if (this != &other) {
      memcpy(data, other.data, SIZE);
    }
    return *this;
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(SIZE);
  }
};

// ============ Context Classes for Fixed-Size Types ============

template<size_t KEY_SIZE, size_t VALUE_SIZE>
class FixedReadContext : public IAsyncContext {
 public:
  typedef FixedKey<KEY_SIZE> key_t;
  typedef FixedValue<VALUE_SIZE> value_t;

  FixedReadContext(const void* key)
    : key_(key), found_(false) {}

  FixedReadContext(const FixedReadContext& other)
    : key_(other.key_), value_(other.value_), found_(other.found_) {}

  // Required interface
  inline const key_t& key() const { return key_; }

  inline void Get(const value_t& value) {
    value_ = value;
    found_ = true;
  }

  inline void GetAtomic(const value_t& value) {
    Get(value);
  }

  // Get results
  bool found() const { return found_; }
  const value_t& value() const { return value_; }
  void* get_value_ptr() { return value_.data; }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    context_copy = new FixedReadContext(*this);
    return Status::Ok;
  }

 private:
  key_t key_;
  value_t value_;
  bool found_;
};

template<size_t KEY_SIZE, size_t VALUE_SIZE>
class FixedUpsertContext : public IAsyncContext {
 public:
  typedef FixedKey<KEY_SIZE> key_t;
  typedef FixedValue<VALUE_SIZE> value_t;

  FixedUpsertContext(const void* key, const void* value)
    : key_(key), value_(value) {}

  FixedUpsertContext(const FixedUpsertContext& other)
    : key_(other.key_), value_(other.value_) {}

  // Required interface
  inline const key_t& key() const { return key_; }

  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  inline void Put(value_t& dest) {
    dest = value_;
  }

  inline bool PutAtomic(value_t& dest) {
    dest = value_;
    return true;
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    context_copy = new FixedUpsertContext(*this);
    return Status::Ok;
  }

 private:
  key_t key_;
  value_t value_;
};

template<size_t KEY_SIZE, size_t VALUE_SIZE>
class FixedRmwContext : public IAsyncContext {
 public:
  typedef FixedKey<KEY_SIZE> key_t;
  typedef FixedValue<VALUE_SIZE> value_t;

  // RMW callback function type
  typedef void (*rmw_callback_t)(const void* old_value, void* new_value, void* user_data);

  FixedRmwContext(const void* key, rmw_callback_t callback, void* user_data)
    : key_(key), callback_(callback), user_data_(user_data) {}

  FixedRmwContext(const FixedRmwContext& other)
    : key_(other.key_), callback_(other.callback_), user_data_(other.user_data_) {}

  // Required interface
  inline const key_t& key() const { return key_; }

  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }

  // Create initial value if key doesn't exist
  inline void RmwInitial(value_t& value) {
    memset(value.data, 0, VALUE_SIZE);
    if (callback_) {
      callback_(nullptr, value.data, user_data_);
    }
  }

  // Copy and modify existing value
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value = old_value;
    if (callback_) {
      callback_(old_value.data, value.data, user_data_);
    }
  }

  // Atomic in-place update
  inline bool RmwAtomic(value_t& value) {
    if (callback_) {
      // For atomic updates, we modify in place
      uint8_t temp[VALUE_SIZE];
      memcpy(temp, value.data, VALUE_SIZE);
      callback_(temp, value.data, user_data_);
    }
    return true;
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    context_copy = new FixedRmwContext(*this);
    return Status::Ok;
  }

 private:
  key_t key_;
  rmw_callback_t callback_;
  void* user_data_;
};

template<size_t KEY_SIZE, size_t VALUE_SIZE>
class FixedDeleteContext : public IAsyncContext {
 public:
  typedef FixedKey<KEY_SIZE> key_t;
  typedef FixedValue<VALUE_SIZE> value_t;

  FixedDeleteContext(const void* key)
    : key_(key) {}

  FixedDeleteContext(const FixedDeleteContext& other)
    : key_(other.key_) {}

  // Required interface
  inline const key_t& key() const { return key_; }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    context_copy = new FixedDeleteContext(*this);
    return Status::Ok;
  }

 private:
  key_t key_;
};

// ============ Store Wrappers for Fixed-Size Types ============

// Default sizes that can be changed at compile time
constexpr size_t DEFAULT_KEY_SIZE = 8;
constexpr size_t DEFAULT_VALUE_SIZE = 100;

template<size_t KEY_SIZE = DEFAULT_KEY_SIZE, size_t VALUE_SIZE = DEFAULT_VALUE_SIZE>
class FixedFasterKv {
 public:
  using Key = FixedKey<KEY_SIZE>;
  using Value = FixedValue<VALUE_SIZE>;
  using disk_t = FileSystemDisk<QueueIoHandler, 1073741824LL>; // 1GB segments
  using store_t = FasterKv<Key, Value, disk_t>;

  store_t* store;
  Guid session_guid;
  bool session_active;

  FixedFasterKv(uint64_t table_size, uint64_t log_size, const std::string& path)
    : session_active(false) {
    store = new store_t(table_size, log_size, path);
    session_guid = store->StartSession();
    session_active = true;
  }

  ~FixedFasterKv() {
    if (session_active && store) {
      store->StopSession();
    }
    delete store;
  }

  Status Upsert(const void* key, const void* value) {
    FixedUpsertContext<KEY_SIZE, VALUE_SIZE> context(key, value);
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Callback for async completion
    };
    Status status = store->Upsert(context, callback, 1);
    store->CompletePending(true);
    return status;
  }

  Status Read(const void* key, void* value) {
    FixedReadContext<KEY_SIZE, VALUE_SIZE> context(key);
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Callback for async completion
    };
    Status status = store->Read(context, callback, 1);
    store->CompletePending(true);

    if (status == Status::Ok && context.found()) {
      memcpy(value, context.value().data, VALUE_SIZE);
    }
    return status;
  }

  Status Rmw(const void* key,
             void (*callback)(const void* old_value, void* new_value, void* user_data),
             void* user_data) {
    FixedRmwContext<KEY_SIZE, VALUE_SIZE> context(key, callback, user_data);
    auto async_callback = [](IAsyncContext* ctxt, Status result) {
      // Callback for async completion
    };
    Status status = store->Rmw(context, async_callback, 1);
    store->CompletePending(true);
    return status;
  }

  Status Delete(const void* key) {
    FixedDeleteContext<KEY_SIZE, VALUE_SIZE> context(key);
    auto callback = [](IAsyncContext* ctxt, Status result) {
      // Callback for async completion
    };
    Status status = store->Delete(context, callback, 1);
    store->CompletePending(true);
    return status;
  }

  void Refresh() {
    store->Refresh();
  }

  void CompletePending(bool wait) {
    store->CompletePending(wait);
  }
};

// Similar wrapper for F2
template<size_t KEY_SIZE = DEFAULT_KEY_SIZE, size_t VALUE_SIZE = DEFAULT_VALUE_SIZE>
class FixedF2Kv {
 public:
  using Key = FixedKey<KEY_SIZE>;
  using Value = FixedValue<VALUE_SIZE>;
  using disk_t = FileSystemDisk<QueueIoHandler, 1073741824LL>; // 1GB segments
  using store_t = F2Kv<Key, Value, disk_t>;

  store_t* store;
  Guid session_guid;
  bool session_active;

  FixedF2Kv(const F2Config& config, const std::string& path)
    : session_active(false) {
    // Create F2 store with provided config
    store = new store_t(config, path);
    session_guid = store->StartSession();
    session_active = true;
  }

  ~FixedF2Kv() {
    if (session_active && store) {
      store->StopSession();
    }
    delete store;
  }

  // Similar methods as FixedFasterKv
  Status Upsert(const void* key, const void* value) {
    FixedUpsertContext<KEY_SIZE, VALUE_SIZE> context(key, value);
    auto callback = [](IAsyncContext* ctxt, Status result) {};
    Status status = store->Upsert(context, callback, 1);
    store->CompletePending(true);
    return status;
  }

  Status Read(const void* key, void* value) {
    FixedReadContext<KEY_SIZE, VALUE_SIZE> context(key);
    auto callback = [](IAsyncContext* ctxt, Status result) {};
    Status status = store->Read(context, callback, 1);
    store->CompletePending(true);

    if (status == Status::Ok && context.found()) {
      memcpy(value, context.value().data, VALUE_SIZE);
    }
    return status;
  }

  void Refresh() {
    store->Refresh();
  }

  void CompletePending(bool wait) {
    store->CompletePending(wait);
  }
};
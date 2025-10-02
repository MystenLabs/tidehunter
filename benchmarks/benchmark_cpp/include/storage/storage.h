#pragma once

#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace benchmark {

// Abstract storage interface matching the Rust Storage trait
class Storage {
public:
    virtual ~Storage() = default;

    // Insert a key-value pair
    virtual void insert(const std::string& key, const std::string& value) = 0;

    // Get value for a key
    virtual std::optional<std::string> get(const std::string& key) = 0;

    // Get values less than the given key (for range queries)
    // Returns up to 'iterations' values
    virtual std::vector<std::string> get_lt(const std::string& key, size_t iterations) = 0;

    // Check if a key exists
    virtual bool exists(const std::string& key) = 0;

    // Get the name of this storage backend
    virtual const char* name() const = 0;

    // Factory method to create storage backends
    static std::unique_ptr<Storage> create(const std::string& backend, const std::string& path);
};

} // namespace benchmark
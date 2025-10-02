#pragma once

#include "storage/storage.h"
#include <lmdb.h>
#include <memory>
#include <string>

namespace benchmark {

class LMDBStorage : public Storage {
private:
    MDB_env* env_;
    MDB_dbi dbi_;
    std::string path_;

    // Config options
    size_t map_size_;
    unsigned int max_readers_;
    unsigned int max_dbs_;

public:
    explicit LMDBStorage(const std::string& path);
    ~LMDBStorage() override;

    void insert(const std::string& key, const std::string& value) override;
    std::optional<std::string> get(const std::string& key) override;
    std::vector<std::string> get_lt(const std::string& key, size_t iterations) override;
    bool exists(const std::string& key) override;
    const char* name() const override { return "LMDB"; }

    // LMDB specific configurations
    void set_map_size(size_t size);
    void set_max_readers(unsigned int readers);
    void set_max_dbs(unsigned int dbs);

private:
    void check_error(int rc, const std::string& operation) const;
};

} // namespace benchmark
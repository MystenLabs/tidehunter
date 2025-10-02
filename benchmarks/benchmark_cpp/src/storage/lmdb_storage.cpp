#include "storage/lmdb_storage.h"
#include <filesystem>
#include <stdexcept>
#include <cstring>

namespace benchmark {

LMDBStorage::LMDBStorage(const std::string& path)
    : path_(path),
      map_size_(1ULL << 40), // 1TB default
      max_readers_(126),
      max_dbs_(1) {

    // Create directory if it doesn't exist
    std::filesystem::create_directories(path);

    // Create and open environment
    int rc = mdb_env_create(&env_);
    check_error(rc, "Creating environment");

    // Set environment parameters
    rc = mdb_env_set_mapsize(env_, map_size_);
    check_error(rc, "Setting map size");

    rc = mdb_env_set_maxreaders(env_, max_readers_);
    check_error(rc, "Setting max readers");

    rc = mdb_env_set_maxdbs(env_, max_dbs_);
    check_error(rc, "Setting max dbs");

    // Open the environment
    unsigned int flags = MDB_NOSUBDIR | MDB_NOSYNC | MDB_WRITEMAP;
    rc = mdb_env_open(env_, path.c_str(), flags, 0664);
    check_error(rc, "Opening environment");

    // Open the database
    MDB_txn* txn;
    rc = mdb_txn_begin(env_, nullptr, 0, &txn);
    check_error(rc, "Beginning transaction");

    rc = mdb_dbi_open(txn, nullptr, MDB_CREATE, &dbi_);
    if (rc != 0) {
        mdb_txn_abort(txn);
        check_error(rc, "Opening database");
    }

    rc = mdb_txn_commit(txn);
    check_error(rc, "Committing transaction");
}

LMDBStorage::~LMDBStorage() {
    if (dbi_) {
        mdb_dbi_close(env_, dbi_);
    }
    if (env_) {
        mdb_env_close(env_);
    }

    // Clean up the directory
    if (!path_.empty()) {
        std::filesystem::remove_all(path_);
    }
}

void LMDBStorage::insert(const std::string& key, const std::string& value) {
    MDB_txn* txn;
    int rc = mdb_txn_begin(env_, nullptr, 0, &txn);
    check_error(rc, "Beginning write transaction");

    MDB_val k, v;
    k.mv_size = key.size();
    k.mv_data = const_cast<char*>(key.data());
    v.mv_size = value.size();
    v.mv_data = const_cast<char*>(value.data());

    rc = mdb_put(txn, dbi_, &k, &v, 0);
    if (rc != 0) {
        mdb_txn_abort(txn);
        check_error(rc, "Putting value");
    }

    rc = mdb_txn_commit(txn);
    check_error(rc, "Committing write transaction");
}

std::optional<std::string> LMDBStorage::get(const std::string& key) {
    MDB_txn* txn;
    int rc = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    check_error(rc, "Beginning read transaction");

    MDB_val k, v;
    k.mv_size = key.size();
    k.mv_data = const_cast<char*>(key.data());

    rc = mdb_get(txn, dbi_, &k, &v);

    std::optional<std::string> result;
    if (rc == 0) {
        result = std::string(static_cast<char*>(v.mv_data), v.mv_size);
    } else if (rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        check_error(rc, "Getting value");
    }

    mdb_txn_commit(txn);
    return result;
}

std::vector<std::string> LMDBStorage::get_lt(const std::string& key, size_t iterations) {
    std::vector<std::string> results;
    results.reserve(iterations);

    MDB_txn* txn;
    int rc = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    check_error(rc, "Beginning read transaction for get_lt");

    MDB_cursor* cursor;
    rc = mdb_cursor_open(txn, dbi_, &cursor);
    if (rc != 0) {
        mdb_txn_abort(txn);
        check_error(rc, "Opening cursor");
    }

    MDB_val k, v;
    k.mv_size = key.size();
    k.mv_data = const_cast<char*>(key.data());

    // Position cursor at or before the key
    rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);

    if (rc == 0 || rc == MDB_NOTFOUND) {
        // Move to previous key if we found exact match or reached end
        rc = mdb_cursor_get(cursor, &k, &v, MDB_PREV);

        // Iterate backwards
        size_t count = 0;
        while (rc == 0 && count < iterations) {
            results.emplace_back(static_cast<char*>(v.mv_data), v.mv_size);
            count++;
            rc = mdb_cursor_get(cursor, &k, &v, MDB_PREV);
        }
    }

    mdb_cursor_close(cursor);
    mdb_txn_commit(txn);

    return results;
}

bool LMDBStorage::exists(const std::string& key) {
    MDB_txn* txn;
    int rc = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    check_error(rc, "Beginning read transaction for exists");

    MDB_val k, v;
    k.mv_size = key.size();
    k.mv_data = const_cast<char*>(key.data());

    rc = mdb_get(txn, dbi_, &k, &v);

    bool result = (rc == 0);
    if (rc != 0 && rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        check_error(rc, "Checking existence");
    }

    mdb_txn_commit(txn);
    return result;
}

void LMDBStorage::set_map_size(size_t size) {
    map_size_ = size;
}

void LMDBStorage::set_max_readers(unsigned int readers) {
    max_readers_ = readers;
}

void LMDBStorage::set_max_dbs(unsigned int dbs) {
    max_dbs_ = dbs;
}

void LMDBStorage::check_error(int rc, const std::string& operation) const {
    if (rc != 0) {
        throw std::runtime_error(operation + " failed: " + mdb_strerror(rc));
    }
}

} // namespace benchmark
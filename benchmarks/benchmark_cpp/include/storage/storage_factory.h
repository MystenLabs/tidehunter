#pragma once

#include "storage/storage.h"
#include "benchmark/config.h"
#include <memory>

namespace benchmark {

class StorageFactory {
public:
    static std::unique_ptr<Storage> create(const StressConfig& config);
};

} // namespace benchmark
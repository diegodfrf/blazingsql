#pragma once

#include <spdlog/spdlog.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "common/WaitingQueue.h"
#include "CacheData.h"
#include "cache_machine/AbstractCacheMachine.h"
#include "bmr/BlazingMemoryResource.h"
namespace ral {
namespace cache {

using Context = blazingdb::manager::Context; 

/**
        @brief A class that represents a Cache Machine on a
        multi-tier (GPU memory, CPU memory, Disk memory) cache system.
*/
class CacheMachine : public AbstractCacheMachine {
public:
  CacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name,
               bool log_timeout = true, int cache_level_override = -1,
               bool is_array_access = false);

  ~CacheMachine();

  void put(size_t index, std::unique_ptr<ral::frame::BlazingTable> table) {
    this->addToCache(std::move(table), this->cache_machine_name + "_" + std::to_string(index), true);
  }

  void put(size_t index, std::unique_ptr<ral::cache::CacheData> cacheData) {
    this->addCacheData(std::move(cacheData), this->cache_machine_name + "_" + std::to_string(index), true);
  }

  virtual bool addToCache(std::unique_ptr<ral::frame::BlazingTable> table,
                          std::string message_id = "", bool always_add = false,
                          const MetadataDictionary& metadata = {},
                          bool use_pinned = false) ;

  virtual bool addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data,
                            std::string message_id = "", bool always_add = false);

  // take the first cacheData in this CacheMachine that it can find (looking in reverse
  // order) that is in the GPU put it in RAM or Disk as oppropriate this function does not
  // change the order of the caches
  virtual size_t downgradeGPUCacheData();

private:
    std::vector<BlazingMemoryResource*> memory_resources;

public:
  static std::shared_ptr<CacheMachine> make_single_machine(
      std::shared_ptr<Context> context, std::string cache_machine_name,
      bool log_timeout = true, int cache_level_override = -1,
      bool is_array_access = false);

  static std::shared_ptr<CacheMachine> make_concatenating_machine(
      std::shared_ptr<Context> context, std::size_t concat_cache_num_bytes,
      int num_bytes_timeout, bool concat_all, std::string cache_machine_name);
};


}  // namespace cache
}  // namespace ral
